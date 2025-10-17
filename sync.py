#!/usr/bin/env python3
"""
Final Ticket Central sync (production-ready)

Key features:
- Tail-window reads per source tab
- Write-back dedupe flag per source row (no global key scans)
- Robust retries/backoff for 429/503
- Throttling (ms sleep) per-read and between sources/flows
- Optional sharding (TOTAL_SHARDS / SHARD_INDEX) to split source processing
- START_FROM_NOW: baseline current rows (stamp flags but do not append)
- Batched appends + batched flag writes
"""

import os
import json
import time
import random
import re
import hashlib
from datetime import datetime, timezone
from typing import List, Tuple

import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError

# ------------------ CONFIG (via ENV) ------------------
MASTER_SPREADSHEET_ID = os.getenv("MASTER_SPREADSHEET_ID")
if not MASTER_SPREADSHEET_ID:
    raise RuntimeError("MASTER_SPREADSHEET_ID env var required")

GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
if not GOOGLE_SERVICE_ACCOUNT_JSON:
    raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON env var required")

MASTER_TICKETS_TAB = os.getenv("MASTER_TICKETS_TAB", "Tickets")
MASTER_SOURCE_TAB = os.getenv("MASTER_SOURCE_TAB", "Source")

# tail and paging
TAIL_WINDOW_ROWS = int(os.getenv("TAIL_WINDOW_ROWS", "3000"))    # how many rows back per source tab
PAGE_ROWS = int(os.getenv("PAGE_ROWS", "3000"))                # page read window
BATCH_APPEND_ROWS = int(os.getenv("BATCH_APPEND_ROWS", "500")) # batch append to master

# throttling & sharding
THROTTLE_MS_PER_READ = int(os.getenv("THROTTLE_MS_PER_READ", "250"))   # ms after each read
SLEEP_BETWEEN_SOURCES_MS = int(os.getenv("SLEEP_BETWEEN_SOURCES_MS", "1000"))
SLEEP_BETWEEN_FLOWS_MS = int(os.getenv("SLEEP_BETWEEN_FLOWS_MS", "3000"))
TOTAL_SHARDS = int(os.getenv("TOTAL_SHARDS", "1"))
SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))

# backoff for transient errors
BACKOFF_BASE_SEC = float(os.getenv("BACKOFF_BASE_SEC", "0.8"))
BACKOFF_MAX_SEC = float(os.getenv("BACKOFF_MAX_SEC", "20.0"))

# baseline mode
START_FROM_NOW = os.getenv("START_FROM_NOW", "false").lower() in ("1", "true", "yes")

# where to write dedupe flags inside each source tab (1-based). Choose a column far to the right.
SYNC_COL_ALL = int(os.getenv("SYNC_COL_ALL", "30"))
SYNC_COL_LI  = int(os.getenv("SYNC_COL_LI",  "30"))

# tabs and required columns
TAB_ALL = "ALL TICKETS (LIVE)"
TAB_LI = "LINKEDIN VIEWS (LIVE)"
START_ROW_ALL = 4   # data starts row 4 (header rows = 3)
START_ROW_LI = 3    # data starts row 3 (header rows = 2)
REQ_ALL = [2, 3]    # B, C
REQ_LI = [2, 3, 4]  # B, C, D
KEYCOLS_ALL = [2, 3]
KEYCOLS_LI = [2, 3, 4]

# mapping to master tickets (1-based)
MAP_ALL = {1:1, 3:2, 10:5, 2:6, 11:7, 12:8, 4:16}
MAP_LI  = {1:1, 2:6, 3:2, 5:8, 4:3}
STATIC_LI = {5: "LinkedIn - LX", 7: "DD"}

MASTER_WIDTH_MIN = max([*MAP_ALL.values(), *MAP_LI.values(), *STATIC_LI.keys(), 16])

# retry sensitivity
RETRYABLE_SNIPPETS = ("[503]", "backenderror", "internal", "service is currently unavailable", "quota", "429")

# ------------------ HELPERS ------------------
def now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

def sleep_ms(ms: int):
    if ms and ms > 0:
        time.sleep(ms / 1000.0)

def backoff_delay(delay: float):
    sleep_ms(int(delay * 1000))
    return min(delay * 2, BACKOFF_MAX_SEC)

def authorize():
    info = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
    scopes = ["https://www.googleapis.com/auth/spreadsheets",
              "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)

def parse_sheet_id(url_or_id: str) -> str:
    m = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", url_or_id)
    return m.group(1) if m else url_or_id.strip()

def col_letter(col_idx: int) -> str:
    s = ""
    while col_idx:
        col_idx, rem = divmod(col_idx - 1, 26)
        s = chr(65 + rem) + s
    return s

# Safe open with retry/backoff
def open_spreadsheet_safe(gc, key, retries=6):
    delay = BACKOFF_BASE_SEC
    for attempt in range(retries):
        try:
            return gc.open_by_key(key)
        except APIError as e:
            msg = str(e).lower()
            if any(token in msg for token in RETRYABLE_SNIPPETS):
                delay = backoff_delay(delay)
                continue
            raise
    return gc.open_by_key(key)

# Safe get worksheet with retry/backoff for transient errors
def get_ws_safe(ss, title, retries=6):
    delay = BACKOFF_BASE_SEC
    for attempt in range(retries):
        try:
            return ss.worksheet(title)
        except gspread.exceptions.WorksheetNotFound:
            return None
        except APIError as e:
            msg = str(e).lower()
            if any(token in msg for token in RETRYABLE_SNIPPETS):
                delay = backoff_delay(delay)
                continue
            raise
    return ss.worksheet(title)

# values_get with backoff; also throttle to avoid per-minute bursts
def values_get_safe(ws, rng: str, retries=6):
    delay = BACKOFF_BASE_SEC
    for attempt in range(retries):
        try:
            vals = ws.get(rng)
            if THROTTLE_MS_PER_READ > 0:
                sleep_ms(THROTTLE_MS_PER_READ)
            return vals
        except APIError as e:
            msg = str(e).lower()
            if any(token in msg for token in RETRYABLE_SNIPPETS):
                delay = backoff_delay(delay)
                continue
            raise
    vals = ws.get(rng)
    if THROTTLE_MS_PER_READ > 0:
        sleep_ms(THROTTLE_MS_PER_READ)
    return vals

def append_rows_safe(ws, rows: List[List[str]], retries=6):
    if not rows:
        return
    delay = BACKOFF_BASE_SEC
    for attempt in range(retries):
        try:
            ws.append_rows(rows, value_input_option="RAW")
            return
        except APIError as e:
            msg = str(e).lower()
            if any(token in msg for token in RETRYABLE_SNIPPETS):
                delay = backoff_delay(delay)
                continue
            raise
    ws.append_rows(rows, value_input_option="RAW")

def map_row_to_master(row: List[str], mapping: dict, statics: dict, width: int) -> List[str]:
    out = [""] * width
    for d_idx, sval in statics.items():
        if 1 <= d_idx <= width:
            out[d_idx - 1] = sval
    for s_idx, d_idx in mapping.items():
        if 1 <= d_idx <= width:
            out[d_idx - 1] = row[s_idx - 1] if s_idx - 1 < len(row) else ""
    return out

# composite key function (not stored centrally; used only to compute)
def make_composite_key(row: List[str], keycols: List[int]) -> str:
    parts = []
    for idx in keycols:
        parts.append((row[idx - 1].strip() if idx - 1 < len(row) else ""))
    basis = "\u241f".join(parts)
    return hashlib.sha1(basis.encode("utf-8")).hexdigest()

# ------------------ SOURCE LIST ------------------
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

# ------------------ CORE PROCESS ------------------
def process_flow_for_source(gc, tickets_ws, master_width, spreadsheet_id, flow) -> Tuple[int, int]:
    if flow == "ALL":
        tab = TAB_ALL
        start_row = START_ROW_ALL
        required = REQ_ALL
        mapping = MAP_ALL
        statics = {}
        sync_col = SYNC_COL_ALL
        keycols = KEYCOLS_ALL
    else:
        tab = TAB_LI
        start_row = START_ROW_LI
        required = REQ_LI
        mapping = MAP_LI
        statics = STATIC_LI
        sync_col = SYNC_COL_LI
        keycols = KEYCOLS_LI

    # open sheet + tab with retry/backoff
    ss = open_spreadsheet_safe(gc, spreadsheet_id)
    ws = get_ws_safe(ss, tab)
    if not ws:
        print(f"[{flow}] {spreadsheet_id}: tab '{tab}' missing - skipping")
        return 0, 0

    # ensure sync column exists and header is set
    ensure_cols = max(sync_col, max(mapping.keys()) if mapping else 1)
    try:
        if ws.col_count < ensure_cols:
            ws.resize(max(ws.row_count, start_row), ensure_cols)
    except Exception:
        # ignore resize errors (we'll attempt reads anyway)
        pass

    hdr_row = start_row - 1
    hdr_cell = f"{col_letter(sync_col)}{hdr_row}"
    try:
        ws.update(values=[[f"__SYNCED_{flow}"]], range_name=f"{hdr_cell}:{hdr_cell}")
    except Exception:
        pass

    max_row = ws.row_count
    if max_row < start_row:
        return 0, 0

    window_start = max(start_row, max_row - TAIL_WINDOW_ROWS + 1)
    max_col_needed = max(max(mapping.keys()) if mapping else 1, max(required))
    max_col = max(max_col_needed, sync_col)
    try:
        if ws.col_count < max_col:
            ws.resize(max(ws.row_count, max_row), max_col)
    except Exception:
        pass

    sync_col_letter = col_letter(sync_col)

    total_appended = 0
    total_scanned = 0
    batch_rows = []
    flag_updates = []  # list of {"range": "AD12:AD12", "values":[["1"]]}

    r = window_start
    while r <= max_row:
        page_end = min(r + PAGE_ROWS - 1, max_row)
        rng = f"{gspread.utils.rowcol_to_a1(r,1)}:{gspread.utils.rowcol_to_a1(page_end, max_col)}"
        values = values_get_safe(ws, rng)

        for i, row in enumerate(values):
            abs_row = r + i
            total_scanned += 1

            # required columns present?
            bad = False
            for idx in required:
                if idx - 1 >= len(row) or str(row[idx - 1]).strip() == "":
                    bad = True
                    break
            if bad:
                continue

            # already flagged?
            synced_val = ""
            if sync_col - 1 < len(row):
                synced_val = str(row[sync_col - 1]).strip()
            if synced_val:
                continue

            # compute composite key if you want (not required for write-flag approach)
            # k = make_composite_key(row, keycols)

            # flag cell
            flag_range = f"{sync_col_letter}{abs_row}:{sync_col_letter}{abs_row}"
            flag_updates.append({"range": flag_range, "values": [["1"]]})

            # baseline mode: only flag (no append)
            if START_FROM_NOW:
                if len(flag_updates) >= BATCH_APPEND_ROWS:
                    try:
                        ws.batch_update(flag_updates)
                    except APIError as e:
                        print("Warning: batch flag write failed; will retry later:", e)
                    flag_updates.clear()
                continue

            # normal: append mapped row then flag
            out = map_row_to_master(row, mapping, statics, master_width)
            batch_rows.append(out)

            if len(batch_rows) >= BATCH_APPEND_ROWS:
                append_rows_safe(tickets_ws, batch_rows)
                # flush flags
                try:
                    ws.batch_update(flag_updates)
                except APIError as e:
                    print("Warning: batch flag write failed; will retry later:", e)
                total_appended += len(batch_rows)
                batch_rows.clear()
                flag_updates.clear()

        r = page_end + 1

    # flush remaining
    if START_FROM_NOW:
        if flag_updates:
            ws.batch_update(flag_updates)
    else:
        if batch_rows:
            append_rows_safe(tickets_ws, batch_rows)
            total_appended += len(batch_rows)
        if flag_updates:
            ws.batch_update(flag_updates)

    return total_appended, total_scanned

# ------------------ MAIN ------------------
def main():
    print("Starting Ticket Central sync:", now_utc())
    gc = authorize()
    master = open_spreadsheet_safe(gc, MASTER_SPREADSHEET_ID)
    tickets_ws = master.worksheet(MASTER_TICKETS_TAB)
    if tickets_ws.col_count < MASTER_WIDTH_MIN:
        try:
            tickets_ws.resize(max(tickets_ws.row_count or 3, 3), MASTER_WIDTH_MIN)
        except Exception:
            pass
    master_width = max(tickets_ws.col_count, MASTER_WIDTH_MIN)

    sources = get_source_ids(master)
    if not sources:
        print("No sources in Source!B2:B — nothing to do.")
        return

    # optional sharding (split sources among shards)
    if TOTAL_SHARDS > 1:
        filtered = [s for i, s in enumerate(sources) if i % TOTAL_SHARDS == SHARD_INDEX]
        print(f"Sharding active: {len(filtered)}/{len(sources)} sources for shard {SHARD_INDEX}/{TOTAL_SHARDS}")
        sources = filtered

    grand_app = grand_scan = 0
    for flow in ("ALL", "LI"):
        flow_app = flow_scan = 0
        for sid in sources:
            try:
                a, s = process_flow_for_source(gc, tickets_ws, master_width, sid, flow)
            except Exception as e:
                # If a source fails repeatedly, skip it but log
                print(f"ERROR processing source {sid} flow {flow}: {e}")
                a, s = 0, 0
            flow_app += a
            flow_scan += s
            # sleep between sources to smooth read rate
            sleep_ms(SLEEP_BETWEEN_SOURCES_MS)
        print(f"[{flow}] scanned={flow_scan} appended={flow_app}")
        grand_app += flow_app
        grand_scan += flow_scan
        # sleep between flows
        sleep_ms(SLEEP_BETWEEN_FLOWS_MS)

    print(f"[ALL FLOWS DONE] scanned={grand_scan} appended={grand_app} — {now_utc()}")

if __name__ == "__main__":
    main()
