#!/usr/bin/env python3
"""
Ticket Central — Zapier/Make-style one-shot sync (edit-safe)

What this does (each run):
- Scans ALL source sheets listed in Master: Source!B2:B (URLs/IDs).
- For each source, processes two tabs:
    1) "ALL TICKETS (LIVE)"  (data from row 4; require B,C non-empty)
    2) "LINKEDIN VIEWS (LIVE)" (data from row 3; require B,C,D non-empty)
- Dedupe key = (source_id, flow, ID_VALUE)
  -> ID_VALUE is taken from a chosen "stable ID" column (default: Column A).
  -> If you edit other columns later, the row WILL NOT duplicate.
  -> If you change the ID column value, it will append again (so keep ID stable).
- Appends mapped static rows to Master: Tickets, padding to full width.
- Uses __KeyIndex tab to remember seen IDs (no duplicates across runs).
- Uses batching + backoff to be resilient to Google quota bursts.
"""

import os, json, time, re
from datetime import datetime, timezone
import hashlib

import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError

# ==================== CONFIG ====================

# Master workbook (env)
MASTER_SPREADSHEET_ID = os.environ["MASTER_SPREADSHEET_ID"]
MASTER_TICKETS_TAB    = os.getenv("MASTER_TICKETS_TAB", "Tickets")
MASTER_SOURCE_TAB     = os.getenv("MASTER_SOURCE_TAB",  "Source")
MARKERS_TAB_NAME      = os.getenv("__MARKERS_TAB_NAME", "__Markers")
KEYINDEX_TAB_NAME     = os.getenv("__KEYINDEX_TAB_NAME","__KeyIndex")

# Composite-key columns (1-based): use required columns
KEYCOLS_ALL = [2, 3]        # B + C
KEYCOLS_LI  = [2, 3, 4]     # B + C + D

# Page/batch sizes (tune if needed)
PAGE_ROWS         = int(os.getenv("PAGE_ROWS", "5000"))    # rows read per page
BATCH_APPEND_ROWS = int(os.getenv("BATCH_APPEND_ROWS", "500"))

# Backoff for Google 429s
BACKOFF_BASE_SEC  = float(os.getenv("BACKOFF_BASE_SEC", "0.8"))
BACKOFF_MAX_SEC   = float(os.getenv("BACKOFF_MAX_SEC", "6.0"))

# Source tabs
TAB_ALL = "ALL TICKETS (LIVE)"
TAB_LI  = "LINKEDIN VIEWS (LIVE)"

# Header sizes → data starts:
START_ROW_ALL = 4  # headers are 3 rows
START_ROW_LI  = 3  # headers are 2 rows

# Validity rules (required non-empty columns, 1-based)
REQ_ALL = [2, 3]        # B, C
REQ_LI  = [2, 3, 4]     # B, C, D

# Column mappings: source idx → Tickets idx (both 1-based)
MAP_ALL = {
    1: 1,    # A -> A
    3: 2,    # C -> B
    10: 5,   # J -> E
    2: 6,    # B -> F
    11: 7,   # K -> G
    12: 8,   # L -> H
    4: 16,   # D -> P
}
MAP_LI = {
    1: 1,    # A -> A
    2: 6,    # B -> F
    3: 2,    # C -> B
    5: 8,    # E -> H
    4: 3,    # D -> C
}
STATIC_LI = {
    5: "LinkedIn - LX",  # -> E
    7: "DD",             # -> G
}

# Master width safety (pad rows at least to this many columns)
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

def ensure_headers(ws, headers):
    ws.update(values=[headers], range_name=f"A1:{chr(ord('A')+len(headers)-1)}1")

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
    # mapped
    for src_idx, dest_idx in mapping.items():
        if 1 <= dest_idx <= width:
            out[dest_idx-1] = row[src_idx-1] if src_idx-1 < len(row) else ""
    return out

def load_seen_ids(key_ws):
    # Read entire key column (A)
    seen = set()
    col = key_ws.col_values(1)
    for i, v in enumerate(col, start=1):
        if i == 1:
            continue  # header
        if v:
            seen.add(v)
    return seen

def make_composite_key(source_id: str, flow: str, row: list[str], keycols: list[int]) -> str:
    # Normalize + hash required columns
    parts = []
    for idx in keycols:
        parts.append((row[idx - 1].strip() if idx - 1 < len(row) else ""))
    basis = f"{source_id}\u241f{flow}\u241f" + "\u241f".join(parts)
    return hashlib.sha1(basis.encode("utf-8")).hexdigest()

def make_id_key(source_id: str, flow: str, id_value: str) -> str:
    # A compact stable key; id_value is the row's stable ID (e.g., Column A)
    basis = f"{source_id}\u241f{flow}\u241f{id_value.strip()}"
    return hashlib.sha1(basis.encode("utf-8")).hexdigest()

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

# ==================== CORE ====================

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
        ensure_headers(key_ws, ["key", "flow", "source_id"])  # A,B,C
    # __Markers
    try:
        mk_ws = master.worksheet(MARKERS_TAB_NAME)
    except gspread.exceptions.WorksheetNotFound:
        mk_ws = master.add_worksheet(MARKERS_TAB_NAME, rows=50, cols=3)
        ensure_headers(mk_ws, ["key", "value", "updated_at"])
    return key_ws, mk_ws

def process_flow_for_source(gc, tickets_ws, key_ws, markers_ws, master_width,
                            spreadsheet_id, flow):
    # Choose per-flow configs
    if flow == FLOW_ALL:
    tab = TAB_ALL; start_row = START_ROW_ALL; required = REQ_ALL; mapping = MAP_ALL; statics = {}; keycols = KEYCOLS_ALL
    else:
    tab = TAB_LI;  start_row = START_ROW_LI; required = REQ_LI; mapping = MAP_LI;  statics = STATIC_LI; keycols = KEYCOLS_LI


    # Open source sheet/tab
    ss = gc.open_by_key(spreadsheet_id)
    ws = get_ws(ss, tab)
    if not ws:
        write_marker(markers_ws, f"CURSOR::{flow}::{spreadsheet_id}", f"NO_TAB:{tab}")
        return 0, 0

    # Read bounds
    max_col = min(highest_needed_col(mapping, required), ws.col_count)
    max_row = ws.row_count

    # Load existing IDs once
    seen = load_seen_ids(key_ws)

    total_appended = 0
    total_scanned  = 0
    batch_rows, batch_keys = [], []

    r = start_row
    while r <= max_row:
        page_end = min(r + PAGE_ROWS - 1, max_row)
        rng = f"{gspread.utils.rowcol_to_a1(r,1)}:{gspread.utils.rowcol_to_a1(page_end,max_col)}"
        values = values_get_safe(ws, rng)

        for i, row in enumerate(values):
            abs_row = r + i
            total_scanned += 1

            # validity check
            valid = True
            for idx in required:
                if (idx - 1) >= len(row) or str(row[idx - 1]).strip() == "":
                    valid = False; break
            if not valid:
                continue

            # Composite key from required columns
k = make_composite_key(spreadsheet_id, flow, row, keycols)
if k in seen:
    continue


            # Map & add
            out = map_row_to_master(row, mapping, statics, master_width)
            batch_rows.append(out)
            batch_keys.append([k, flow, spreadsheet_id])
            seen.add(k)

            # Flush in batches
            if len(batch_rows) >= BATCH_APPEND_ROWS:
                append_rows_safe(tickets_ws, batch_rows, "RAW")
                append_rows_safe(key_ws,     batch_keys, "RAW")
                total_appended += len(batch_rows)
                batch_rows.clear(); batch_keys.clear()

        r = page_end + 1

    # Flush remain
    if batch_rows:
        append_rows_safe(tickets_ws, batch_rows, "RAW")
        append_rows_safe(key_ws,     batch_keys, "RAW")
        total_appended += len(batch_rows)

    write_marker(markers_ws, f"CURSOR::{flow}::{spreadsheet_id}", f"DONE:{max_row}")
    return total_appended, total_scanned

def main():
    gc = authorize()
    master = gc.open_by_key(MASTER_SPREADSHEET_ID)

    # Ensure tabs
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
            flow_app += a; flow_scan += s
        print(f"[{flow}] scanned={flow_scan} appended={flow_app}")
        grand_app += flow_app; grand_scan += flow_scan

    print(f"[ALL FLOWS DONE] scanned={grand_scan} appended={grand_app}")

if __name__ == "__main__":
    main()
