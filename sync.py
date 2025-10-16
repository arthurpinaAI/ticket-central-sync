#!/usr/bin/env python3
"""
Ticket Central – Resumable Multi-Source Google Sheets → Master Tickets appender
- Two flows: ALL TICKETS (LIVE) and LINKEDIN VIEWS (LIVE)
- Per-source, per-flow row cursors in __Markers (no duplicates, safe resume)
- Reads only needed columns and bounded row windows
- Throttling + exponential backoff to avoid 429 quota errors
- Time-budgeted; spreads work across frequent cron runs
- Optional INIT_FROM_NOW to skip history for first-time sources
"""

import os, json, time, re
from datetime import datetime, timezone
import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError

# ─────────────────────────── CONFIG VIA ENV ───────────────────────────
MASTER_SPREADSHEET_ID = os.environ["MASTER_SPREADSHEET_ID"]               # Master workbook (has Tickets + Source)
MASTER_TICKETS_TAB    = os.getenv("MASTER_TICKETS_TAB", "Tickets")
MASTER_SOURCE_TAB     = os.getenv("MASTER_SOURCE_TAB",  "Source")
MARKERS_TAB_NAME      = os.getenv("__MARKERS_TAB_NAME", "__Markers")

# Chunking & safety
TIME_BUDGET_SEC       = int(os.getenv("TIME_BUDGET_SEC", "330"))        # < GH job timeout; we exit cleanly
CHUNK_ROWS_PER_SOURCE = int(os.getenv("CHUNK_ROWS", "800"))             # rows scanned per source per pass
INIT_FROM_NOW         = os.getenv("INIT_FROM_NOW", "false").lower() in ("1","true","yes")

# Quota-friendly pacing
THROTTLE_MS           = int(os.getenv("THROTTLE_MS", "200"))            # pause after each chunk
MAX_SOURCES_PER_RUN   = int(os.getenv("MAX_SOURCES_PER_RUN", "15"))     # limit sources per flow per run

# Source tabs
TAB_ALL   = "ALL TICKETS (LIVE)"
TAB_LI    = "LINKEDIN VIEWS (LIVE)"

# Data starts after headers in source sheets
START_ROW_ALL = 4   # headers = 3 rows (data starts at row 4)
START_ROW_LI  = 3   # headers = 2 rows (data starts at row 3)

# Validity: required columns must be non-empty (1-based indexes)
REQ_ALL = [2, 3]       # B, C
REQ_LI  = [2, 3, 4]    # B, C, D

# Column mappings: source idx (1-based) -> Tickets idx (1-based)
MAP_ALL = {
    1: 1,   # A -> A(1)
    3: 2,   # C -> B(2)
    10: 5,  # J -> E(5)
    2: 6,   # B -> F(6)
    11: 7,  # K -> G(7)
    12: 8,  # L -> H(8)
    4: 16,  # D -> P(16)
}
# LINKEDIN mappings + statics
MAP_LI = {
    1: 1,  # A -> A(1)
    2: 6,  # B -> F(6)
    3: 2,  # C -> B(2)
    5: 8,  # E -> H(8)
    4: 3,  # D -> C(3)
}
STATIC_LI = {
    5: "LinkedIn - LX",  # dest E(5)
    7: "DD",             # dest G(7)
}

# Master width implied by mappings (max destination index). We ensure at least this many columns.
MASTER_WIDTH_MIN = max([*MAP_ALL.values(), *MAP_LI.values(), *STATIC_LI.keys(), 16])

# Flow keys for markers
FLOW_ALL = "ALL"
FLOW_LI  = "LI"

# ─────────────────────────── AUTH & HELPERS ───────────────────────────

def _now_utc_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def auth_client():
    info = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"])
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)


def ensure_sheet(ws, min_rows=1, min_cols=1):
    rows, cols = ws.row_count, ws.col_count
    grow_r = max(0, min_rows - rows)
    grow_c = max(0, min_cols - cols)
    if grow_r or grow_c:
        ws.resize(rows + grow_r if grow_r else rows, cols + grow_c if grow_c else cols)


def get_or_create_markers(master):
    try:
        ws = master.worksheet(MARKERS_TAB_NAME)
    except gspread.exceptions.WorksheetNotFound:
        ws = master.add_worksheet(MARKERS_TAB_NAME, rows=50, cols=3)
        ws.update(values=[["key","value","updated_at"]], range_name="A1:C1")
    return ws


def read_markers(markers_ws):
    values = markers_ws.get_all_values()
    idx = {}
    for r in values[1:]:
        if len(r) < 1:
            continue
        key = r[0]
        val = r[1] if len(r) > 1 else ""
        idx[key] = val
    return idx


def write_marker(markers_ws, key, value):
    # Upsert by key in col A
    values = markers_ws.get_all_values()
    key_to_row = {}
    for i, r in enumerate(values, start=1):
        if i == 1:
            continue
        if r and r[0] == key:
            key_to_row[key] = i
            break
    ts = _now_utc_iso()
    if key in key_to_row:
        row = key_to_row[key]
        markers_ws.update(values=[[key, str(value), ts]], range_name=f"A{row}:C{row}")
    else:
        markers_ws.append_row([key, str(value), ts], value_input_option="RAW")


def parse_sheet_id(url_or_id):
    m = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", url_or_id)
    return m.group(1) if m else url_or_id.strip()


def get_ws_by_title(ss, title):
    try:
        return ss.worksheet(title)
    except gspread.exceptions.WorksheetNotFound:
        return None


def get_master_width(tickets_ws):
    # Prefer actual sheet width; ensure at least mapping minimum.
    return max(MASTER_WIDTH_MIN, tickets_ws.col_count)


def read_source_list(master, source_tab):
    ws = master.worksheet(source_tab)
    col_b = ws.col_values(2)  # Column B
    urls = []
    for i, v in enumerate(col_b, start=1):
        if i < 2:   # row 1 header; row 2+ are URLs
            continue
        if v and v.strip():
            urls.append(v.strip())
    return [parse_sheet_id(x) for x in urls]


def get_cursor_key(spreadsheet_id, flow):
    return f"CURSOR::{flow}::{spreadsheet_id}"


def get_queue_key(flow):
    return f"SYNC_CURSOR_{flow}"


def get_ws_required_and_mapping(flow):
    if flow == FLOW_ALL:
        return TAB_ALL, START_ROW_ALL, REQ_ALL, MAP_ALL, {}
    else:
        return TAB_LI, START_ROW_LI, REQ_LI, MAP_LI, STATIC_LI


def is_row_valid(row, required_cols):
    # row is 0-based list; required_cols are 1-based
    for idx in required_cols:
        val = row[idx-1] if idx-1 < len(row) else ""
        if str(val).strip() == "":
            return False
    return True


def map_row_to_master(row, mapping, static_map, master_width):
    out = [""] * master_width
    # statics first
    for dest_idx, sval in static_map.items():
        if 1 <= dest_idx <= master_width:
            out[dest_idx-1] = sval
    # mapped fields
    for src_idx, dest_idx in mapping.items():
        if 1 <= dest_idx <= master_width:
            val = row[src_idx-1] if src_idx-1 < len(row) else ""
            out[dest_idx-1] = val
    return out


def highest_needed_col(mapping, required_cols, _static_map_unused):
    max_src = max(mapping.keys()) if mapping else 1
    max_req = max(required_cols) if required_cols else 1
    return max(max_src, max_req)


def values_get_safe(ws, rng, retries=5, base_delay=0.8):
    """Read a range with exponential backoff on 429 quota errors."""
    delay = base_delay
    for _ in range(retries):
        try:
            return ws.get(rng)
        except APIError as e:
            msg = str(e).lower()
            if "quota exceeded" in msg or "429" in msg:
                time.sleep(delay)
                delay = min(delay * 2, 6.0)
                continue
            raise
    return ws.get(rng)


def read_rows_window(ws, start_row, max_col, from_row_inclusive, limit_rows):
    """Read a bounded window of rows, clamped to the sheet grid size (rows & cols)."""
    sheet_max_rows = ws.row_count
    sheet_max_cols = ws.col_count
    if from_row_inclusive > sheet_max_rows:
        return [], []

    to_row = min(from_row_inclusive + limit_rows - 1, sheet_max_rows)
    if to_row < from_row_inclusive:
        return [], []

    eff_max_col = min(max_col, sheet_max_cols)
    rng = f"{gspread.utils.rowcol_to_a1(from_row_inclusive, 1)}:{gspread.utils.rowcol_to_a1(to_row, eff_max_col)}"
    vals = values_get_safe(ws, rng)
    base_row_numbers = list(range(from_row_inclusive, to_row + 1))
    return base_row_numbers, vals


# ─────────────────────────── CORE PROCESSING ───────────────────────────

def process_flow_for_source(gc, tickets_ws, markers_ws, master_width, spreadsheet_id, flow, time_guard_deadline):
    tab, start_row, required, mapping, statics = get_ws_required_and_mapping(flow)
    # open source
    ss = gc.open_by_key(spreadsheet_id)
    ws = get_ws_by_title(ss, tab)
    if not ws:
        # No such tab: mark as fully processed to avoid retry storms
        write_marker(markers_ws, get_cursor_key(spreadsheet_id, flow), start_row - 1)
        return 0, 0

    # Initialize cursor
    markers = read_markers(markers_ws)
    ckey = get_cursor_key(spreadsheet_id, flow)
    cursor = markers.get(ckey)
    if cursor is None or cursor == "":
        if INIT_FROM_NOW:
            # Skip history: use grid row_count as last processed
            cursor_val = max(start_row - 1, ws.row_count)
        else:
            cursor_val = start_row - 1
        write_marker(markers_ws, ckey, cursor_val)
        cursor = str(cursor_val)
    last_done = int(cursor)

    # Highest needed source column for efficient reads
    max_col = highest_needed_col(mapping, required, statics)
    appended = 0
    scanned = 0

    while time.time() < time_guard_deadline:
        from_row = last_done + 1
        base_rows, raw = read_rows_window(ws, start_row, max_col, from_row, CHUNK_ROWS_PER_SOURCE)
        if not raw:
            break

        to_append = []
        for idx, row in enumerate(raw):
            absolute_row = base_rows[idx]
            scanned += 1
            if absolute_row < start_row:
                last_done = absolute_row
                continue
            if is_row_valid(row, required):
                out = map_row_to_master(row, mapping, statics, master_width)
                to_append.append(out)
            last_done = absolute_row

        if to_append:
            # append_rows may also 429; retry lightly
            delay = 0.8
            for _ in range(5):
                try:
                    tickets_ws.append_rows(to_append, value_input_option="RAW")
                    appended += len(to_append)
                    break
                except APIError as e:
                    msg = str(e).lower()
                    if "quota exceeded" in msg or "429" in msg:
                        time.sleep(delay)
                        delay = min(delay * 2, 6.0)
                        continue
                    raise

        # Update cursor after each chunk
        write_marker(markers_ws, ckey, last_done)

        # Gentle throttle to stay under per-minute limits
        if THROTTLE_MS > 0:
            time.sleep(THROTTLE_MS / 1000.0)

        # If fewer than chunk came back, likely end of sheet
        if len(raw) < CHUNK_ROWS_PER_SOURCE:
            break

    return appended, scanned


def process_all(gc):
    t0 = time.time()
    deadline = t0 + TIME_BUDGET_SEC

    master = gc.open_by_key(MASTER_SPREADSHEET_ID)
    tickets_ws = master.worksheet(MASTER_TICKETS_TAB)
    source_ws  = master.worksheet(MASTER_SOURCE_TAB)
    markers_ws = get_or_create_markers(master)

    # Ensure minimal shape for Tickets & Markers
    ensure_sheet(tickets_ws, min_rows=3, min_cols=MASTER_WIDTH_MIN)
    ensure_sheet(markers_ws, min_rows=5, min_cols=3)

    master_width = get_master_width(tickets_ws)
    sources = read_source_list(master, MASTER_SOURCE_TAB)
    if not sources:
        print("No sources found in Source!B2:B — nothing to do.")
        return

    # Process each flow with queue progression
    for flow in (FLOW_ALL, FLOW_LI):
        markers = read_markers(markers_ws)
        qkey = get_queue_key(flow)
        try:
            start_idx = int(markers.get(qkey, "0"))
        except Exception:
            start_idx = 0

        i = start_idx
        total_appended = 0
        total_scanned = 0
        processed_sources = 0

        while time.time() < deadline and i < len(sources):
            sid = sources[i]
            a, s = process_flow_for_source(gc, tickets_ws, markers_ws, master_width, sid, flow, deadline)
            total_appended += a
            total_scanned  += s
            i += 1
            write_marker(markers_ws, qkey, i)  # move queue forward per completed source

            processed_sources += 1
            if processed_sources >= MAX_SOURCES_PER_RUN:
                break

        print(f"[{flow}] scanned={total_scanned} appended={total_appended} next_source_index={i}/{len(sources)}")

        # If finished all sources for this flow, wrap to 0 for next run
        if i >= len(sources):
            write_marker(markers_ws, qkey, 0)


def main():
    gc = auth_client()
    process_all(gc)


if __name__ == "__main__":
    main()
