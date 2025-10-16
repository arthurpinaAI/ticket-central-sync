#!/usr/bin/env python3
"""
Ticket Central — Zapier/Make-style one-shot sync (write-back dedupe flag)

Why this stops 429s:
- For each source tab we do exactly ONE tail-window read (values_get).
- No separate key-sheet reads. Dedupe is a simple on-row flag in the same tab.
- Writes are batched: one append_rows to Master + one batch_update to set flags.

Behavior each run:
- Master: Source!B2:B lists source spreadsheet URLs/IDs.
- For each source:
   * Tab "ALL TICKETS (LIVE)": data from row 4; require B,C non-empty; dedupe flag col "__SYNCED_ALL".
   * Tab "LINKEDIN VIEWS (LIVE)": data from row 3; require B,C,D non-empty; dedupe flag col "__SYNCED_LI".
- Start-Now baseline: set START_FROM_NOW=true for a run → we only stamp the flags (no appends).
- Normal runs: append mapped rows exactly once per source row (flag prevents duplicates forever,
  even if the row is edited later).

Config (env):
- MASTER_SPREADSHEET_ID (required)
- MASTER_TICKETS_TAB (default "Tickets"), MASTER_SOURCE_TAB (default "Source")
- TAIL_WINDOW_ROWS (default 3000) — how far back to scan in each source tab
- PAGE_ROWS (default 3000) — page size inside the window
- BATCH_APPEND_ROWS (default 500) — append/write batch size
- BACKOFF_BASE_SEC (0.8), BACKOFF_MAX_SEC (6.0)
- START_FROM_NOW (true/false) — stamp flags without appending
- SYNC_COL_ALL (default 30), SYNC_COL_LI (default 30) — 1-based column index to place the flag
"""

import os, json, time, re
from datetime import datetime, timezone
from typing import List, Tuple, Dict

import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError

# ─────────── ENV ───────────
MASTER_SPREADSHEET_ID = os.environ["MASTER_SPREADSHEET_ID"]
MASTER_TICKETS_TAB    = os.getenv("MASTER_TICKETS_TAB", "Tickets")
MASTER_SOURCE_TAB     = os.getenv("MASTER_SOURCE_TAB",  "Source")

TAIL_WINDOW_ROWS      = int(os.getenv("TAIL_WINDOW_ROWS", "3000"))
PAGE_ROWS             = int(os.getenv("PAGE_ROWS", "3000"))
BATCH_APPEND_ROWS     = int(os.getenv("BATCH_APPEND_ROWS", "500"))

BACKOFF_BASE_SEC      = float(os.getenv("BACKOFF_BASE_SEC", "0.8"))
BACKOFF_MAX_SEC       = float(os.getenv("BACKOFF_MAX_SEC", "6.0"))

START_FROM_NOW        = os.getenv("START_FROM_NOW", "false").lower() in ("1","true","yes")

# Helper flag column positions (1-based). Put them far to the right to avoid touching your data.
SYNC_COL_ALL          = int(os.getenv("SYNC_COL_ALL", "30"))  # AD
SYNC_COL_LI           = int(os.getenv("SYNC_COL_LI",  "30"))  # AD

# Tabs & rules
TAB_ALL, START_ROW_ALL, REQ_ALL = "ALL TICKETS (LIVE)", 4, [2, 3]       # B, C
TAB_LI,  START_ROW_LI,  REQ_LI  = "LINKEDIN VIEWS (LIVE)", 3, [2, 3, 4] # B, C, D

# Column mappings (source→Tickets)
MAP_ALL = {1:1, 3:2, 10:5, 2:6, 11:7, 12:8, 4:16}
MAP_LI  = {1:1, 2:6, 3:2, 5:8, 4:3}
STATIC_LI = {5:"LinkedIn - LX", 7:"DD"}

MASTER_WIDTH_MIN = max([*MAP_ALL.values(), *MAP_LI.values(), *STATIC_LI.keys(), 16])

# ─────────── UTILS ───────────
def now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

def authorize() -> gspread.Client:
    info = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"])
    creds = Credentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive"],
    )
    return gspread.authorize(creds)

def parse_sheet_id(url_or_id: str) -> str:
    m = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", url_or_id)
    return m.group(1) if m else url_or_id.strip()

def get_ws(ss: gspread.Spreadsheet, title: str):
    try:
        return ss.worksheet(title)
    except gspread.exceptions.WorksheetNotFound:
        return None
import random
from gspread.exceptions import APIError

RETRYABLE_SNIPPETS = ("[503]", "backendError", "internal", "service is currently unavailable")

def open_spreadsheet_safe(gc, key, retries=6, base=0.8, cap=12.0):
    delay = base
    for _ in range(retries):
        try:
            return gc.open_by_key(key)
        except APIError as e:
            msg = str(e).lower()
            if any(x.lower() in msg for x in RETRYABLE_SNIPPETS):
                time.sleep(delay + random.random() * 0.5)
                delay = min(delay * 2, cap)
                continue
            raise
    # last attempt
    return gc.open_by_key(key)

def get_ws_safe(ss, title, retries=6, base=0.8, cap=12.0):
    delay = base
    for _ in range(retries):
        try:
            return ss.worksheet(title)
        except gspread.exceptions.WorksheetNotFound:
            return None
        except APIError as e:
            msg = str(e).lower()
            if any(x.lower() in msg for x in RETRYABLE_SNIPPETS):
                time.sleep(delay + random.random() * 0.5)
                delay = min(delay * 2, cap)
                continue
            raise
    return ss.worksheet(title)

def ensure_sheet_size(ws, min_rows: int, min_cols: int):
    if ws.row_count < min_rows or ws.col_count < min_cols:
        ws.resize(max(ws.row_count, min_rows), max(ws.col_count, min_cols))

def highest_needed_col(mapping: dict, required_cols: List[int], sync_col: int) -> int:
    m = max(mapping.keys()) if mapping else 1
    r = max(required_cols) if required_cols else 1
    return max(m, r, sync_col)

def values_get_safe(ws, rng: str, retries: int = 6):
    delay = BACKOFF_BASE_SEC
    for _ in range(retries):
        try:
            return ws.get(rng)
        except APIError as e:
            msg = str(e).lower()
            if "quota exceeded" in msg or "429" in msg:
                time.sleep(delay); delay = min(delay * 2, BACKOFF_MAX_SEC); continue
            raise
    return ws.get(rng)

def append_rows_safe(ws, rows: List[List[str]], value_input_option="RAW", retries: int = 6):
    if not rows: return
    delay = BACKOFF_BASE_SEC
    for _ in range(retries):
        try:
            ws.append_rows(rows, value_input_option=value_input_option); return
        except APIError as e:
            msg = str(e).lower()
            if "quota exceeded" in msg or "429" in msg:
                time.sleep(delay); delay = min(delay * 2, BACKOFF_MAX_SEC); continue
            raise
    ws.append_rows(rows, value_input_option=value_input_option)

def col_letter(col_idx: int) -> str:
    s = ""
    while col_idx:
        col_idx, rem = divmod(col_idx-1, 26)
        s = chr(65+rem) + s
    return s

def map_row_to_master(row: List[str], mapping: dict, statics: dict, width: int) -> List[str]:
    out = [""] * width
    for d, v in statics.items():
        if 1 <= d <= width: out[d-1] = v
    for src, dst in mapping.items():
        if 1 <= dst <= width:
            out[dst-1] = row[src-1] if src-1 < len(row) else ""
    return out

# ─────────── MASTER SOURCE LIST ───────────
def get_source_ids(master: gspread.Spreadsheet) -> List[str]:
    ws = master.worksheet(MASTER_SOURCE_TAB)
    col_b = ws.col_values(2)
    out = []
    for i, v in enumerate(col_b, start=1):
        if i < 2: continue
        if v and v.strip(): out.append(parse_sheet_id(v.strip()))
    return out

# ─────────── CORE FLOW ───────────
def process_flow_for_source(
    gc: gspread.Client,
    tickets_ws,
    master_width: int,
    spreadsheet_id: str,
    flow: str,
) -> Tuple[int, int]:
    if flow == "ALL":
        tab, start_row, required, mapping, statics, sync_col, sync_header = (
            TAB_ALL, START_ROW_ALL, REQ_ALL, MAP_ALL, {}, SYNC_COL_ALL, "__SYNCED_ALL"
        )
    else:
        tab, start_row, required, mapping, statics, sync_col, sync_header = (
            TAB_LI, START_ROW_LI, REQ_LI, MAP_LI, STATIC_LI, SYNC_COL_LI, "__SYNCED_LI"
        )

   ss = open_spreadsheet_safe(gc, spreadsheet_id)
ws = get_ws_safe(ss, tab)

    if ws is None:
        print(f"[{flow}] {spreadsheet_id}: tab '{tab}' not found — skipping.")
        return 0, 0

    # Ensure the sheet has the sync column and header cell
    ensure_sheet_size(ws, min_rows=start_row, min_cols=sync_col)
    hdr_row = start_row - 1  # last header row
    hdr_cell = f"{col_letter(sync_col)}{hdr_row}"
    try:
        ws.update(values=[[sync_header]], range_name=f"{hdr_cell}:{hdr_cell}")
    except Exception:
        pass  # non-fatal

    # Tail window
    max_row = ws.row_count
    if max_row < start_row: return 0, 0
    window_start = max(start_row, max_row - TAIL_WINDOW_ROWS + 1)
    max_col = min(max(ws.col_count, sync_col), ws.col_count)  # ensure we read existing cols
    max_col = max(max_col, highest_needed_col(mapping, required, sync_col))
    ensure_sheet_size(ws, min_rows=max_row, min_cols=max_col)

    sync_col_letter = col_letter(sync_col)

    total_appended = 0
    total_scanned  = 0
    batch_rows: List[List[str]] = []
    flag_updates_ranges: List[Dict] = []  # for batch_update

    r = window_start
    while r <= max_row:
        page_end = min(r + PAGE_ROWS - 1, max_row)
        rng = f"{gspread.utils.rowcol_to_a1(r,1)}:{gspread.utils.rowcol_to_a1(page_end,max_col)}"
        values = values_get_safe(ws, rng)

        for i, row in enumerate(values):
            abs_row = r + i
            total_scanned += 1

            # Required columns
            ok = True
            for idx in required:
                if (idx - 1) >= len(row) or str(row[idx - 1]).strip() == "":
                    ok = False; break
            if not ok:
                continue

            # Already synced?
            synced_val = ""
            if sync_col - 1 < len(row):
                synced_val = str(row[sync_col - 1]).strip()
            if synced_val:
                continue  # skip already synced

            if START_FROM_NOW:
                # Baseline: only flag, no append
                rg = f"{sync_col_letter}{abs_row}:{sync_col_letter}{abs_row}"
                flag_updates_ranges.append({"range": rg, "values": [["1"]]})
            else:
                # Append then flag
                out = map_row_to_master(row, mapping, statics, master_width)
                batch_rows.append(out)
                rg = f"{sync_col_letter}{abs_row}:{sync_col_letter}{abs_row}"
                flag_updates_ranges.append({"range": rg, "values": [["1"]]})

                if len(batch_rows) >= BATCH_APPEND_ROWS:
                    append_rows_safe(tickets_ws, batch_rows, "RAW")
                    # Batch flag updates in one request
                    ws.batch_update([{"range": o["range"], "values": o["values"]} for o in flag_updates_ranges])
                    total_appended += len(batch_rows)
                    batch_rows.clear()
                    flag_updates_ranges.clear()

        r = page_end + 1

    # Flush remaining
    if START_FROM_NOW:
        if flag_updates_ranges:
            ws.batch_update([{"range": o["range"], "values": o["values"]} for o in flag_updates_ranges])
    else:
        if batch_rows:
            append_rows_safe(tickets_ws, batch_rows, "RAW")
            ws.batch_update([{"range": o["range"], "values": o["values"]} for o in flag_updates_ranges])
            total_appended += len(batch_rows)

    return total_appended, total_scanned

def main():
    gc = authorize()
    master = gc.open_by_key(MASTER_SPREADSHEET_ID)

    tickets_ws = master.worksheet(MASTER_TICKETS_TAB)
    if tickets_ws.col_count < MASTER_WIDTH_MIN:
        tickets_ws.resize(tickets_ws.row_count or 3, MASTER_WIDTH_MIN)
    master_width = max(tickets_ws.col_count, MASTER_WIDTH_MIN)

    sources = get_source_ids(master)
    if not sources:
        print("No sources in Source!B2:B — nothing to do."); return

    grand_app = grand_scan = 0
    for flow in ("ALL", "LI"):
        flow_app = flow_scan = 0
        for sid in sources:
            a, s = process_flow_for_source(gc, tickets_ws, master_width, sid, flow)
            flow_app += a; flow_scan += s
        print(f"[{flow}] scanned={flow_scan} appended={flow_app}")
        grand_app += flow_app; grand_scan += flow_scan

    print(f"[ALL FLOWS DONE] scanned={grand_scan} appended={grand_app}")

if __name__ == "__main__":
    main()
