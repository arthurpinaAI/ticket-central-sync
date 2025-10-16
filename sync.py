#!/usr/bin/env python3
"""
Ticket Central — Zapier/Make-style one-shot sync (composite-key dedupe)

Behavior each run:
- Scans ALL source sheets listed in Master: Source!B2:B (URLs/IDs).
- For each source, processes two tabs:
    1) "ALL TICKETS (LIVE)"       (data from row 4; require B,C non-empty)
    2) "LINKEDIN VIEWS (LIVE)"    (data from row 3; require B,C,D non-empty)
- Unique row key = SHA1(source_id + flow + required-columns):
    ALL key = B + C
    LI  key = B + C + D
- Appends mapped static rows to Master: Tickets, padding to full width.
- __KeyIndex tab stores seen keys (so each unique row is appended ONCE ever).
- Robust batching + backoff for Google rate limits.

“Start now” (skip history):
- Set env START_FROM_NOW=true on the FIRST run.
- Script will baseline: record the current keys into __KeyIndex WITHOUT appending them,
  so future runs only append rows added after *now*.
- Baseline markers prevent re-baselining the same source.
"""

import os, json, time, re, hashlib
from datetime import datetime, timezone
from typing import List, Tuple

import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError

# ==================== CONFIG VIA ENV ====================

MASTER_SPREADSHEET_ID = os.environ["MASTER_SPREADSHEET_ID"]
MASTER_TICKETS_TAB    = os.getenv("MASTER_TICKETS_TAB", "Tickets")
MASTER_SOURCE_TAB     = os.getenv("MASTER_SOURCE_TAB",  "Source")
MARKERS_TAB_NAME      = os.getenv("__MARKERS_TAB_NAME", "__Markers")
KEYINDEX_TAB_NAME     = os.getenv("__KEYINDEX_TAB_NAME","__KeyIndex")

# One-shot full sweep defaults
PAGE_ROWS             = int(os.getenv("PAGE_ROWS", "5000"))     # rows per page read
BATCH_APPEND_ROWS     = int(os.getenv("BATCH_APPEND_ROWS", "500"))

# Backoff (survive 429 rate limits)
BACKOFF_BASE_SEC      = float(os.getenv("BACKOFF_BASE_SEC", "0.8"))
BACKOFF_MAX_SEC       = float(os.getenv("BACKOFF_MAX_SEC", "6.0"))

# Start-now (skip history on first run for unseen sources)
START_FROM_NOW        = os.getenv("START_FROM_NOW", "false").lower() in ("1","true","yes")

# Flow/tab details
TAB_ALL = "ALL TICKETS (LIVE)"
TAB_LI  = "LINKEDIN VIEWS (LIVE)"

START_ROW_ALL = 4  # headers = 3 rows
START_ROW_LI  = 3  # headers = 2 rows

# Required non-empty source columns (1-based)
REQ_ALL = [2, 3]        # B, C
REQ_LI  = [2, 3, 4]     # B, C, D

# Composite key columns (required columns)
KEYCOLS_ALL = [2, 3]        # B + C
KEYCOLS_LI  = [2, 3, 4]     # B + C + D

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

# Master width (pad at least this many columns)
MASTER_WIDTH_MIN = max([*MAP_ALL.values(), *MAP_LI.values(), *STATIC_LI.keys(), 16])

FLOW_ALL = "ALL"
FLOW_LI  = "LI"

# ==================== HELPERS ====================

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

def ensure_headers(ws, headers: List[str]):
    ws.update(values=[headers], range_name=f"A1:{chr(ord('A')+len(headers)-1)}1")

def ensure_sheet_size(ws, min_rows: int, min_cols: int):
    if ws.row_count < min_rows or ws.col_count < min_cols:
        ws.resize(max(ws.row_count, min_rows), max(ws.col_count, min_cols))

def highest_needed_col(mapping, required_cols) -> int:
    m = max(mapping.keys()) if mapping else 1
    r = max(required_cols) if required_cols else 1
    return max(m, r)

def values_get_safe(ws, rng: str, retries: int = 6):
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

def append_rows_safe(ws, rows: List[List[str]], value_input_option="RAW", retries: int = 6):
    if not rows:
        return
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

def map_row_to_master(row: List[str], mapping: dict, statics: dict, width: int) -> List[str]:
    out = [""] * width
    # statics first
    for dest_idx, sval in statics.items():
        if 1 <= dest_idx <= width:
            out[dest_idx - 1] = sval
    # mapped fields
    for src_idx, dest_idx in mapping.items():
        if 1 <= dest_idx <= width:
            out[dest_idx - 1] = row[src_idx - 1] if src_idx - 1 < len(row) else ""
    return out

def load_seen_keys(key_ws) -> set:
    seen = set()
    col = key_ws.col_values(1)  # A = key
    for i, v in enumerate(col, start=1):
        if i == 1:
            continue  # header
        if v:
            seen.add(v)
    return seen

def make_composite_key(source_id: str, flow: str, row: List[str], keycols: List[int]) -> str:
    # Normalize + hash required columns
    parts = []
    for idx in keycols:
        parts.append((row[idx - 1].strip() if idx - 1 < len(row) else ""))
    basis = f"{source_id}\u241f{flow}\u241f" + "\u241f".join(parts)
    return hashlib.sha1(basis.encode("utf-8")).hexdigest()

def write_marker(markers_ws, key: str, value: str):
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

# ==================== CORE ====================

def get_source_ids(master: gspread.Spreadsheet) -> List[str]:
    ws = master.worksheet(MASTER_SOURCE_TAB)
    col_b = ws.col_values(2)
    ids = []
    for i, v in enumerate(col_b, start=1):
        if i < 2:
            continue
        if v and v.strip():
            ids.append(parse_sheet_id(v.strip()))
    return ids

def ensure_admin_tabs(master: gspread.Spreadsheet):
    # __KeyIndex
    try:
        key_ws = master.worksheet(KEYINDEX_TAB_NAME)
    except gspread.exceptions.WorksheetNotFound:
        key_ws = master.add_worksheet(KEYINDEX_TAB_NAME, rows=100, cols=3)
        ensure_headers(key_ws, ["key", "flow", "source_id"])  # A,B,C
    # __Markers
    try:
        mk_ws = master.worksheet(MARKERS_TAB_NAME)
    except gspread.exceptions.WorksheetNotFound:
        mk_ws = master.add_worksheet(MARKERS_TAB_NAME, rows=50, cols=3)
        ensure_headers(mk_ws, ["key", "value", "updated_at"])
    return key_ws, mk_ws

def process_flow_for_source(
    gc: gspread.Client,
    tickets_ws,
    key_ws,
    markers_ws,
    master_width: int,
    spreadsheet_id: str,
    flow: str
) -> Tuple[int, int]:
    # Select per-flow config
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
        write_marker(markers_ws, f"BASELINE::{flow}::{spreadsheet_id}", f"NO_TAB:{tab}")
        return 0, 0

    max_col = min(highest_needed_col(mapping, required), ws.col_count)
    max_row = ws.row_count

    # Load existing seen keys once
    seen = load_seen_keys(key_ws)

    # If START_FROM_NOW is on and this source+flow hasn't been baselined, baseline it:
    baseline_key = f"BASELINE::{flow}::{spreadsheet_id}"
    already_baselined = False
    try:
        markers_map = {r[0]: r[1] for r in markers_ws.get_all_values()[1:] if r and r[0]}
        already_baselined = baseline_key in markers_map
    except Exception:
        pass

    total_appended = 0
    total_scanned  = 0
    batch_rows, batch_keys = [], []

    r = start_row
    while r <= max_row:
        page_end = min(r + PAGE_ROWS - 1, max_row)
        rng = f"{gspread.utils.rowcol_to_a1(r, 1)}:{gspread.utils.rowcol_to_a1(page_end, max_col)}"
        values = values_get_safe(ws, rng)

        for i, row in enumerate(values):
            abs_row = r + i
            total_scanned += 1

            # validity (required columns must be non-empty)
            valid = True
            for idx in required:
                if (idx - 1) >= len(row) or str(row[idx - 1]).strip() == "":
                    valid = False
                    break
            if not valid:
                continue

            # Composite key from required columns
            k = make_composite_key(spreadsheet_id, flow, row, keycols)

            if START_FROM_NOW and not already_baselined:
                # Baseline mode: record key only (no append)
                if k not in seen:
                    batch_keys.append([k, flow, spreadsheet_id])
                    seen.add(k)
                if len(batch_keys) >= BATCH_APPEND_ROWS:
                    append_rows_safe(key_ws, batch_keys, "RAW")
                    batch_keys.clear()
                continue  # don't append to Tickets during baseline

            # Normal mode: append only unseen keys
            if k in seen:
                continue

            out = map_row_to_master(row, mapping, statics, master_width)
            batch_rows.append(out)
            batch_keys.append([k, flow, spreadsheet_id])
            seen.add(k)

            if len(batch_rows) >= BATCH_APPEND_ROWS:
                append_rows_safe(tickets_ws, batch_rows, "RAW")
                append_rows_safe(key_ws,     batch_keys, "RAW")
                total_appended += len(batch_rows)
                batch_rows.clear()
                batch_keys.clear()

        r = page_end + 1

    # Flush remaining
    if START_FROM_NOW and not already_baselined:
        if batch_keys:
            append_rows_safe(key_ws, batch_keys, "RAW")
            batch_keys.clear()
        write_marker(markers_ws, baseline_key, f"DONE:{max_row}")
    else:
        if batch_rows:
            append_rows_safe(tickets_ws, batch_rows, "RAW")
            append_rows_safe(key_ws,     batch_keys, "RAW")
            total_appended += len(batch_rows)

        write_marker(markers_ws, f"CURSOR::{flow}::{spreadsheet_id}", f"DONE:{max_row}")

    return total_appended, total_scanned

def main():
    gc = authorize()
    master = gc.open_by_key(MASTER_SPREADSHEET_ID)

    # Ensure master tabs
    tickets_ws = master.worksheet(MASTER_TICKETS_TAB)
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
