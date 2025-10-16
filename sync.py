#!/usr/bin/env python3
"""
Ticket Central — Zapier/Make-style one-shot sync

Behavior:
- Scans EVERY listed source sheet in one run (no artificial chunk-per-run limits).
- Two flows per source: "ALL TICKETS (LIVE)" and "LINKEDIN VIEWS (LIVE)".
- Appends to master Tickets once per NEW ROW using a key-based dedupe index (__KeyIndex).
- Validates required columns before mapping.
- Pads rows to master width.
- Batches reads/writes and uses exponential backoff to survive 429 quota bursts.
- Safe to run daily on schedule (or manually).

Sheets:
- Master workbook tabs:
  * Tickets (destination)
  * Source (source spreadsheet URLs/IDs in column B from row 2)
  * __KeyIndex (auto-created; columns: key, flow, source_id)
  * __Markers (optional, for basic progress info)
- Source workbooks each contain:
  * ALL TICKETS (LIVE): header rows=3 (data starts at row 4)
  * LINKEDIN VIEWS (LIVE): header rows=2 (data starts at row 3)
"""

import os, json, time, re, hashlib
from datetime import datetime, timezone

import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError

# -------------------- ENV / CONFIG --------------------
MASTER_SPREADSHEET_ID = os.environ["MASTER_SPREADSHEET_ID"]
MASTER_TICKETS_TAB    = os.getenv("MASTER_TICKETS_TAB", "Tickets")
MASTER_SOURCE_TAB     = os.getenv("MASTER_SOURCE_TAB",  "Source")
MARKERS_TAB_NAME      = os.getenv("__MARKERS_TAB_NAME", "__Markers")
KEYINDEX_TAB_NAME     = os.getenv("__KEYINDEX_TAB_NAME","__KeyIndex")

# Page/batch sizes (tune if needed)
PAGE_ROWS             = int(os.getenv("PAGE_ROWS", "5000"))   # rows per read page from a source tab
BATCH_APPEND_ROWS     = int(os.getenv("BATCH_APPEND_ROWS", "500"))  # rows per append to Tickets/KeyIndex

# Quota-safety (still runs to completion; these only slow down slightly when needed)
BACKOFF_BASE_SEC      = float(os.getenv("BACKOFF_BASE_SEC", "0.8"))
BACKOFF_MAX_SEC       = float(os.getenv("BACKOFF_MAX_SEC", "6.0"))

# Flow/tab details
TAB_ALL = "ALL TICKETS (LIVE)"
TAB_LI  = "LINKEDIN VIEWS (LIVE)"

START_ROW_ALL = 4  # headers = 3 rows
START_ROW_LI  = 3  # headers = 2 rows

# Required non-empty source columns (1-based)
REQ_ALL = [2, 3]        # B, C
REQ_LI  = [2, 3, 4]     # B, C, D

# Column mappings: source idx → Tickets idx (both 1-based)
MAP_ALL = {
    1: 1,    # A(1) -> A(1)
    3: 2,    # C(3) -> B(2)
    10: 5,   # J(10)-> E(5)
    2: 6,    # B(2) -> F(6)
    11: 7,   # K(11)-> G(7)
    12: 8,   # L(12)-> H(8)
    4: 16,   # D(4) -> P(16)
}

MAP_LI = {
    1: 1,    # A(1) -> A(1)
    2: 6,    # B(2) -> F(6)
    3: 2,    # C(3) -> B(2)
    5: 8,    # E(5) -> H(8)
    4: 3,    # D(4) -> C(3)
}
STATIC_LI = {
    5: "LinkedIn - LX",  # -> E(5) static
    7: "DD",             # -> G(7) static
}

# Master width (pad to this many columns at minimum)
MASTER_WIDTH_MIN = max([*MAP_ALL.values(), *MAP_LI.values(), *STATIC_LI.keys(), 16])

# Key columns for dedupe (Zapier-style “seen row once”)
KEYCOLS_ALL = [1, 2, 3]          # A+B+C
KEYCOLS_LI  = [1, 2, 3, 4]       # A+B+C+D

FLOW_ALL = "ALL"
FLOW_LI  = "LI"

# -------------------- UTILS --------------------
def now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

def authorize() -> gspread.Client:
    info = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"])
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)

def parse_sheet_id(url_or_id: str) -> str:
    m = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", url_or_id)
    return m.group(1) if m else url_or_id.strip()

def get_ws(ss: gspread.Spreadsheet, title: str):
    try:
        return ss.worksheet(title)
    except gspread.exceptions.WorksheetNotFound:
        return None

def ensure_headers(ws, headers, range_a1="A1"):
    ws.update(values=[headers], range_name=f"{range_a1}:{chr(ord('A')+len(headers)-1)}1")

def ensure_sheet_size(ws, min_rows, min_cols):
    if ws.row_count < min_rows or ws.col_count < min_cols:
        ws.resize(max(ws.row_count, min_rows), max(ws.col_count, min_cols))

def highest_needed_col(mapping, required_cols):
    m = max(mapping.keys()) if mapping else 1
    r = max(required_cols) if required_cols else 1
    return max(m, r)

def values_get_safe(ws, rng, retries=6):
    delay = BACKOFF_BASE_SEC
    for _ in range(retries):
        try:
            return ws.get(rng)
        except APIError as e:
            msg = str(e).lower()
            if "quota exceeded" in msg or "429" in msg:
                time.sleep(delay)
                delay = min(delay * 2, BACKOFF_MAX_SEC)
                continue
            raise
    return ws.get(rng)

def append_rows_safe(ws, rows, value_input_option="RAW", retries=6):
    delay = BACKOFF_BASE_SEC
    for _ in range(retries):
        try:
            ws.append_rows(rows, value_input_option=value_input_option)
            return
        except APIError as e:
            msg = str(e).lower()
            if "quota exceeded" in msg or "429" in msg:
                time.sleep(delay)
                delay = min(delay * 2, BACKOFF_MAX_SEC)
                continue
            raise
    ws.append_rows(rows, value_input_option=value_input_option)

def map_row_to_master(row, mapping, statics, width):
    out = [""] * width
    # statics
    for dest_idx, sval in statics.items():
        if 1 <= dest_idx <= width:
            out[dest_idx-1] = sval
    # mapped fields
    for src_idx, dest_idx in mapping.items():
        if 1 <= dest_idx <= width:
            out[dest_idx-1] = row[src_idx-1] if src_idx-1 < len(row) else ""
    return out

def make_key(row, keycols):
    parts = [(row[i-1] if i-1 < len(row) else "").strip() for i in keycols]
    blob = "\u241f".join(parts)  # unit separator
    return hashlib.sha1(blob.encode("utf-8")).hexdigest()

def load_seen_keys(key_ws):
    # Reads entire key column (A). If this grows huge (millions), consider sharding later.
    seen = set()
    col = key_ws.col_values(1)
    for idx, v in enumerate(col, start=1):
        if idx == 1:
            continue  # header
        if v:
            seen.add(v)
    return seen

def write_marker(markers_ws, key, value):
    # Upsert by key in __Markers (A=key, B=value, C=updated_at)
    values = markers_ws.get_all_values()
    row_found = None
    for i, r in enumerate(values, start=1):
        if i == 1:
            continue
        if r and r[0] == key:
            row_found = i
            break
    ts = now_utc()
    if row_found:
        markers_ws.update(values=[[key, str(value), ts]], range_name=f"A{row_found}:C{row_found}")
    else:
        markers_ws.append_row([key, str(value), ts], value_input_option="RAW")

# -------------------- CORE --------------------
def get_source_ids(master):
    ws = master.worksheet(MASTER_SOURCE_TAB)
    col_b = ws.col_values(2)
    ids = []
    for i, v in enumerate(col_b, start=1):
        if i < 2:
            continue
        if v and v.strip():
            ids.append(parse_sheet_id(v.strip()))
    return ids

def ensure_admin_tabs(master):
    # __KeyIndex
    try:
        key_ws = master.worksheet(KEYINDEX_TAB_NAME)
    except gspread.exceptions.WorksheetNotFound:
        key_ws = master.add_worksheet(KEYINDEX_TAB_NAME, rows=100, cols=3)
        ensure_headers(key_ws, ["key", "flow", "source_id"], "A1")
    # __Markers
    try:
        mk_ws = master.worksheet(MARKERS_TAB_NAME)
    except gspread.exceptions.WorksheetNotFound:
        mk_ws = master.add_worksheet(MARKERS_TAB_NAME, rows=50, cols=3)
        ensure_headers(mk_ws, ["key", "value", "updated_at"], "A1")
    return key_ws, mk_ws

def process_flow_for_source(gc, tickets_ws, key_ws, markers_ws, master_width,
                            spreadsheet_id, flow):
    # Flow config
    if flow == FLOW_ALL:
        tab = TAB_ALL
        start_row = START_ROW_ALL
        required = REQ_ALL
        mapping = MAP_ALL
        statics = {}
        keycols = KEYCOLS_ALL
    else:
        tab = TAB_LI
        start_row = START_ROW_LI
        required = REQ_LI
        mapping = MAP_LI
        statics = STATIC_LI
        keycols = KEYCOLS_LI

    # Open source sheet/tab
    ss = gc.open_by_key(spreadsheet_id)
    ws = get_ws(ss, tab)
    if not ws:
        # no such tab — just mark and move on
        write_marker(markers_ws, f"CURSOR::{flow}::{spreadsheet_id}", f"NO_TAB:{tab}")
        return 0, 0

    # Prepare read bounds
    max_col_needed = highest_needed_col(mapping, required)
    max_col = min(max_col_needed, ws.col_count)
    max_row = ws.row_count

    # Preload seen keys (Zapier-style dedupe)
    seen = load_seen_keys(key_ws)

    total_appended = 0
    total_scanned = 0
    batch_rows = []
    batch_keys = []

    # Page through the ENTIRE sheet (one run), but only up to max needed columns
    r = start_row
    while r <= max_row:
        page_end = min(r + PAGE_ROWS - 1, max_row)
        rng = f"{gspread.utils.rowcol_to_a1(r, 1)}:{gspread.utils.rowcol_to_a1(page_end, max_col)}"
        values = values_get_safe(ws, rng)  # list of lists
        # values list length can be shorter than (page_end - r + 1) for trailing blanks; compute absolute row per item
        for i, row in enumerate(values):
            abs_row = r + i
            total_scanned += 1

            # validity
            ok = True
            for req_idx in required:
                if (req_idx - 1) >= len(row) or str(row[req_idx - 1]).strip() == "":
                    ok = False
                    break
            if not ok:
                continue

            # Key & dedupe
            k = make_key(row, keycols)
            if k in seen:
                continue

            out = map_row_to_master(row, mapping, statics, master_width)
            batch_rows.append(out)
            batch_keys.append([k, flow, spreadsheet_id])
            seen.add(k)

            # Append in batches to reduce API calls
            if len(batch_rows) >= BATCH_APPEND_ROWS:
                append_rows_safe(tickets_ws, batch_rows, value_input_option="RAW")
                append_rows_safe(key_ws,     batch_keys, value_input_option="RAW")
                total_appended += len(batch_rows)
                batch_rows.clear()
                batch_keys.clear()

        r = page_end + 1

    # Flush remaining
    if batch_rows:
        append_rows_safe(tickets_ws, batch_rows, value_input_option="RAW")
        append_rows_safe(key_ws,     batch_keys, value_input_option="RAW")
        total_appended += len(batch_rows)

    # Write a marker so you can see it finished this tab
    write_marker(markers_ws, f"CURSOR::{flow}::{spreadsheet_id}", f"DONE:{max_row}")

    return total_appended, total_scanned

def main():
    gc = authorize()
    master = gc.open_by_key(MASTER_SPREADSHEET_ID)
    tickets_ws = master.worksheet(MASTER_TICKETS_TAB)

    # Ensure master width (pad safety)
    ensure_sheet_size(tickets_ws, min_rows=3, min_cols=MASTER_WIDTH_MIN)
    master_width = max(tickets_ws.col_count, MASTER_WIDTH_MIN)

    key_ws, markers_ws = ensure_admin_tabs(master)
    sources = get_source_ids(master)
    if not sources:
        print("No sources in Source!B2:B — nothing to do.")
        return

    grand_app = grand_scan = 0
    for flow in (FLOW_ALL, FLOW_LI):
        flow_app = flow_scan = 0
        for sid in sources:
            a, s = process_flow_for_source(gc, tickets_ws, key_ws, markers_ws, master_width, sid, flow)
            flow_app += a
            flow_scan += s
        print(f"[{flow}] scanned={flow_scan} appended={flow_app}")
        grand_app += flow_app
        grand_scan += flow_scan

    print(f"[ALL FLOWS DONE] scanned={grand_scan} appended={grand_app}")

if __name__ == "__main__":
    main()
