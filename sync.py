#!/usr/bin/env python3
"""
Ticket Central — Zapier/Make-style one-shot sync (write-back dedupe + retry-safe)

Key design:
- Each source sheet tab is scanned only once per run (tail window read).
- Dedupe stored directly in the same tab (flag column "__SYNCED_ALL"/"__SYNCED_LI").
- No extra key sheets → no quota blowups.
- Automatic retry/backoff for 429/503.
- START_FROM_NOW=true => mark existing rows as synced (no append).
"""

import os, json, time, re, random
from datetime import datetime, timezone
from typing import List, Tuple
import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError

# ────────── CONFIG ──────────
MASTER_SPREADSHEET_ID = os.environ["MASTER_SPREADSHEET_ID"]
MASTER_TICKETS_TAB = os.getenv("MASTER_TICKETS_TAB", "Tickets")
MASTER_SOURCE_TAB = os.getenv("MASTER_SOURCE_TAB", "Source")

TAIL_WINDOW_ROWS = int(os.getenv("TAIL_WINDOW_ROWS", "3000"))
PAGE_ROWS = int(os.getenv("PAGE_ROWS", "3000"))
BATCH_APPEND_ROWS = int(os.getenv("BATCH_APPEND_ROWS", "500"))
BACKOFF_BASE_SEC = float(os.getenv("BACKOFF_BASE_SEC", "0.8"))
BACKOFF_MAX_SEC = float(os.getenv("BACKOFF_MAX_SEC", "6.0"))
START_FROM_NOW = os.getenv("START_FROM_NOW", "false").lower() in ("1", "true", "yes")

# Flag column positions (1-based, far to right)
SYNC_COL_ALL = int(os.getenv("SYNC_COL_ALL", "30"))
SYNC_COL_LI = int(os.getenv("SYNC_COL_LI", "30"))

# Flow details
TAB_ALL, START_ROW_ALL, REQ_ALL = "ALL TICKETS (LIVE)", 4, [2, 3]
TAB_LI, START_ROW_LI, REQ_LI = "LINKEDIN VIEWS (LIVE)", 3, [2, 3, 4]

# Mappings to Tickets (1-based)
MAP_ALL = {1: 1, 3: 2, 10: 5, 2: 6, 11: 7, 12: 8, 4: 16}
MAP_LI = {1: 1, 2: 6, 3: 2, 5: 8, 4: 3}
STATIC_LI = {5: "LinkedIn - LX", 7: "DD"}
MASTER_WIDTH_MIN = max([*MAP_ALL.values(), *MAP_LI.values(), *STATIC_LI.keys(), 16])

RETRYABLE_SNIPPETS = ("[503]", "backenderror", "internal", "service is currently unavailable", "quota", "429")

# ────────── HELPERS ──────────
def now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

def authorize() -> gspread.Client:
    info = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"])
    creds = Credentials.from_service_account_info(
        info,
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    return gspread.authorize(creds)

def parse_sheet_id(url_or_id: str) -> str:
    m = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", url_or_id)
    return m.group(1) if m else url_or_id.strip()

def backoff_delay(delay: float):
    time.sleep(delay + random.random() * 0.5)
    return min(delay * 2, BACKOFF_MAX_SEC)

def open_spreadsheet_safe(gc, key, retries=6, base=0.8):
    delay = base
    for _ in range(retries):
        try:
            return gc.open_by_key(key)
        except APIError as e:
            msg = str(e).lower()
            if any(x in msg for x in RETRYABLE_SNIPPETS):
                delay = backoff_delay(delay)
                continue
            raise
    return gc.open_by_key(key)

def get_ws_safe(ss, title, retries=6, base=0.8):
    delay = base
    for _ in range(retries):
        try:
            return ss.worksheet(title)
        except gspread.exceptions.WorksheetNotFound:
            return None
        except APIError as e:
            msg = str(e).lower()
            if any(x in msg for x in RETRYABLE_SNIPPETS):
                delay = backoff_delay(delay)
                continue
            raise
    return ss.worksheet(title)

def ensure_sheet_size(ws, min_rows: int, min_cols: int):
    if ws.row_count < min_rows or ws.col_count < min_cols:
        ws.resize(max(ws.row_count, min_rows), max(ws.col_count, min_cols))

def col_letter(idx: int) -> str:
    s = ""
    while idx:
        idx, rem = divmod(idx - 1, 26)
        s = chr(65 + rem) + s
    return s

def values_get_safe(ws, rng: str, retries=6):
    delay = BACKOFF_BASE_SEC
    for _ in range(retries):
        try:
            return ws.get(rng)
        except APIError as e:
            msg = str(e).lower()
            if any(x in msg for x in RETRYABLE_SNIPPETS):
                delay = backoff_delay(delay)
                continue
            raise
    return ws.get(rng)

def append_rows_safe(ws, rows: List[List[str]], retries=6):
    if not rows:
        return
    delay = BACKOFF_BASE_SEC
    for _ in range(retries):
        try:
            ws.append_rows(rows, value_input_option="RAW")
            return
        except APIError as e:
            msg = str(e).lower()
            if any(x in msg for x in RETRYABLE_SNIPPETS):
                delay = backoff_delay(delay)
                continue
            raise
    ws.append_rows(rows, value_input_option="RAW")

def map_row_to_master(row: List[str], mapping: dict, statics: dict, width: int) -> List[str]:
    out = [""] * width
    for d, v in statics.items():
        if 1 <= d <= width:
            out[d - 1] = v
    for s_idx, d_idx in mapping.items():
        if 1 <= d_idx <= width:
            out[d_idx - 1] = row[s_idx - 1] if s_idx - 1 < len(row) else ""
    return out

def get_source_ids(master: gspread.Spreadsheet) -> List[str]:
    ws = master.worksheet(MASTER_SOURCE_TAB)
    col_b = ws.col_values(2)
    return [parse_sheet_id(v.strip()) for i, v in enumerate(col_b, start=1) if i >= 2 and v.strip()]

# ────────── MAIN FLOW ──────────
def process_flow_for_source(gc, tickets_ws, master_width, spreadsheet_id, flow) -> Tuple[int, int]:
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
    if not ws:
        print(f"[{flow}] {spreadsheet_id}: tab '{tab}' not found — skipping.")
        return 0, 0

    ensure_sheet_size(ws, min_rows=start_row, min_cols=sync_col)
    hdr_cell = f"{col_letter(sync_col)}{start_row - 1}"
    try:
        ws.update(values=[[sync_header]], range_name=f"{hdr_cell}:{hdr_cell}")
    except Exception:
        pass

    max_row = ws.row_count
    if max_row < start_row:
        return 0, 0
    window_start = max(start_row, max_row - TAIL_WINDOW_ROWS + 1)
    max_col = max(ws.col_count, sync_col)
    ensure_sheet_size(ws, min_rows=max_row, min_cols=max_col)
    sync_col_letter = col_letter(sync_col)

    total_appended = 0
    total_scanned = 0
    batch_rows = []
    flag_updates = []

    r = window_start
    while r <= max_row:
        page_end = min(r + PAGE_ROWS - 1, max_row)
        rng = f"{gspread.utils.rowcol_to_a1(r,1)}:{gspread.utils.rowcol_to_a1(page_end,max_col)}"
        values = values_get_safe(ws, rng)

        for i, row in enumerate(values):
            abs_row = r + i
            total_scanned += 1

            # Required cols
            if any((idx - 1 >= len(row)) or (str(row[idx - 1]).strip() == "") for idx in required):
                continue

            synced_val = str(row[sync_col - 1]).strip() if sync_col - 1 < len(row) else ""
            if synced_val:
                continue

            flag_cell = f"{sync_col_letter}{abs_row}"
            flag_updates.append({"range": f"{flag_cell}:{flag_cell}", "values": [["1"]]})

            if START_FROM_NOW:
                continue  # only flagging

            out = map_row_to_master(row, mapping, statics, master_width)
            batch_rows.append(out)

            if len(batch_rows) >= BATCH_APPEND_ROWS:
                append_rows_safe(tickets_ws, batch_rows)
                ws.batch_update(flag_updates)
                total_appended += len(batch_rows)
                batch_rows.clear()
                flag_updates.clear()

        r = page_end + 1

    if START_FROM_NOW:
        if flag_updates:
            ws.batch_update(flag_updates)
    else:
        if batch_rows:
            append_rows_safe(tickets_ws, batch_rows)
            ws.batch_update(flag_updates)
            total_appended += len(batch_rows)

    return total_appended, total_scanned

def main():
    gc = authorize()
    master = gc.open_by_key(MASTER_SPREADSHEET_ID)
    tickets_ws = master.worksheet(MASTER_TICKETS_TAB)
    ensure_sheet_size(tickets_ws, min_rows=3, min_cols=MASTER_WIDTH_MIN)
    master_width = max(tickets_ws.col_count, MASTER_WIDTH_MIN)

    sources = get_source_ids(master)
    if not sources:
        print("No sources found.")
        return

    grand_app = grand_scan = 0
    for flow in ("ALL", "LI"):
        flow_app = flow_scan = 0
        for sid in sources:
            a, s = process_flow_for_source(gc, tickets_ws, master_width, sid, flow)
            flow_app += a
            flow_scan += s
        print(f"[{flow}] scanned={flow_scan} appended={flow_app}")
        grand_app += flow_app
        grand_scan += flow_scan

    print(f"[DONE] scanned={grand_scan} appended={grand_app}")

if __name__ == "__main__":
    main()
