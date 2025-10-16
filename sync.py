#!/usr/bin/env python3
"""
Ticket Central — Zapier/Make-style one-shot sync
(per-source tail-window + tail-keys only; 429-resistant; start-now supported)

Key ideas:
- For each source tab we only scan the LAST TAIL_WINDOW_ROWS (default 3000).
- For dedupe, we only load the LAST KEYS_TAIL rows from the per-source key tab
  (default 5000), tracked via a tiny "last_row" marker in the header (no full col read).
- Composite key = required columns:
    ALL: B + C
    LI : B + C + D
- Start-Now mode: record keys (no append) on first run; future runs append only new keys.

Tabs created in EACH source spreadsheet:
  __SyncedKeys_ALL: A="key", B1="last_row" holding last populated row index (int)
  __SyncedKeys_LI : same
"""

import os, json, time, re, hashlib
from datetime import datetime, timezone
from typing import List, Tuple, Set

import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError

# ====== ENV CONFIG ======
MASTER_SPREADSHEET_ID = os.environ["MASTER_SPREADSHEET_ID"]
MASTER_TICKETS_TAB    = os.getenv("MASTER_TICKETS_TAB", "Tickets")
MASTER_SOURCE_TAB     = os.getenv("MASTER_SOURCE_TAB",  "Source")

# Tail sizes
TAIL_WINDOW_ROWS      = int(os.getenv("TAIL_WINDOW_ROWS", "3000"))   # rows to scan in source tab
KEYS_TAIL             = int(os.getenv("KEYS_TAIL", "5000"))          # how many recent keys to read from keys tab

# Paging & batching
PAGE_ROWS             = int(os.getenv("PAGE_ROWS", "3000"))
BATCH_APPEND_ROWS     = int(os.getenv("BATCH_APPEND_ROWS", "500"))

# Backoff
BACKOFF_BASE_SEC      = float(os.getenv("BACKOFF_BASE_SEC", "0.8"))
BACKOFF_MAX_SEC       = float(os.getenv("BACKOFF_MAX_SEC", "6.0"))

# Start-Now (baseline) for unseen sources
START_FROM_NOW        = os.getenv("START_FROM_NOW", "false").lower() in ("1","true","yes")

# Flow/tab names
TAB_ALL = "ALL TICKETS (LIVE)"
TAB_LI  = "LINKEDIN VIEWS (LIVE)"

# Data starts after headers
START_ROW_ALL = 4  # 3 header rows
START_ROW_LI  = 3  # 2 header rows

# Required columns (1-based)
REQ_ALL = [2, 3]       # B, C
REQ_LI  = [2, 3, 4]    # B, C, D

# Composite-key columns (same as required)
KEYCOLS_ALL = [2, 3]
KEYCOLS_LI  = [2, 3, 4]

# Mappings → Tickets (1-based)
MAP_ALL = {1:1, 3:2, 10:5, 2:6, 11:7, 12:8, 4:16}
MAP_LI  = {1:1, 2:6, 3:2, 5:8, 4:3}
STATIC_LI = {5:"LinkedIn - LX", 7:"DD"}

# Master width (pad at least to this many columns)
MASTER_WIDTH_MIN = max([*MAP_ALL.values(), *MAP_LI.values(), *STATIC_LI.keys(), 16])

# Per-source key tabs
KEYTAB_ALL = "__SyncedKeys_ALL"
KEYTAB_LI  = "__SyncedKeys_LI"

# ====== UTILS ======
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

def highest_needed_col(mapping: dict, required_cols: List[int]) -> int:
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
    for dest_idx, sval in statics.items():
        if 1 <= dest_idx <= width:
            out[dest_idx - 1] = sval
    for src_idx, dest_idx in mapping.items():
        if 1 <= dest_idx <= width:
            out[dest_idx - 1] = row[src_idx - 1] if src_idx - 1 < len(row) else ""
    return out

def make_composite_key(row: List[str], keycols: List[int]) -> str:
    parts = []
    for idx in keycols:
        parts.append((row[idx - 1].strip() if idx - 1 < len(row) else ""))
    return hashlib.sha1(("\u241f".join(parts)).encode("utf-8")).hexdigest()

# ====== KEYS TAB (tail-only) ======
def get_or_create_keys_tab(ss: gspread.Spreadsheet, name: str):
    ws = get_ws(ss, name)
    if ws is None:
        ws = ss.add_worksheet(name, rows=100, cols=2)
        ensure_headers(ws, ["key", "last_row"])       # A1, B1
        ws.update(values=[["", "1"]], range_name="A2:B2")  # B2=1 marker
        return ws

    # --- auto-upgrade existing 1-column tabs ---
    if ws.col_count < 2:
        ws.resize(max(ws.row_count, 100), 2)

    # ensure header row present
    hdr = ws.get("A1:B1") or []
    a1 = (hdr[0][0] if len(hdr) >= 1 and len(hdr[0]) >= 1 else "").strip()
    b1 = (hdr[0][1] if len(hdr) >= 1 and len(hdr[0]) >= 2 else "").strip()
    if a1 != "key" or b1 != "last_row":
        ws.update(values=[["key", "last_row"]], range_name="A1:B1")

    # ensure marker B2 exists and is an int
    rng = ws.get("B2:B2")
    b2 = (rng[0][0] if rng and rng[0] else "").strip()
    if not b2.isdigit():
        # compute last_row as current last non-empty row in col A (small read)
        col_a = ws.col_values(1)
        last = max(1, len([v for v in col_a if v]))  # includes header
        ws.update(values=[[str(last)]], range_name="B2:B2")

    return ws


def get_keys_last_row(keys_ws) -> int:
    # guarantee 2 columns
    if keys_ws.col_count < 2:
        keys_ws.resize(max(keys_ws.row_count, 100), 2)
        keys_ws.update(values=[["key", "last_row"]], range_name="A1:B1")
        keys_ws.update(values=[["", "1"]], range_name="A2:B2")
        return 1

    rng = keys_ws.get("B2:B2")  # tiny single cell
    try:
        if rng and rng[0] and rng[0][0].strip().isdigit():
            return max(1, int(rng[0][0].strip()))
    except Exception:
        pass
    # fallback: compute from col A (still small for key tabs)
    col_a = keys_ws.col_values(1)
    return max(1, len([v for v in col_a if v]))


def set_keys_last_row(keys_ws, last_row: int):
    keys_ws.update(values=[[str(last_row)]], range_name="B2:B2")

def load_keys_tail(keys_ws, tail: int) -> Set[str]:
    """
    Load only the last 'tail' keys using the last_row marker.
    """
    last_row = get_keys_last_row(keys_ws)
    start = max(2, last_row - tail + 1)
    rng = f"A{start}:A{last_row}"
    vals = values_get_safe(keys_ws, rng)
    seen = set()
    for r in vals:
        if r and r[0]:
            seen.add(r[0])
    return seen

def append_keys_and_update_marker(keys_ws, new_keys: List[str]):
    if not new_keys:
        return
    # current marker
    last_row = get_keys_last_row(keys_ws)
    rows = [[k] for k in new_keys]
    append_rows_safe(keys_ws, rows, "RAW")
    # new last row = old last + count appended
    keys_ws.update(values=[[str(last_row + len(rows))]], range_name="B2:B2")


# ====== MASTER SOURCE LIST ======
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

# ====== CORE FLOW ======
def process_flow_for_source(
    gc: gspread.Client,
    master_tickets,
    master_width: int,
    spreadsheet_id: str,
    flow: str,
) -> Tuple[int, int]:
    # Flow config
    if flow == "ALL":
        tab, start_row, required, mapping, statics, keycols, keytab = (
            TAB_ALL, START_ROW_ALL, REQ_ALL, MAP_ALL, {}, KEYCOLS_ALL, KEYTAB_ALL
        )
    else:
        tab, start_row, required, mapping, statics, keycols, keytab = (
            TAB_LI, START_ROW_LI, REQ_LI, MAP_LI, STATIC_LI, KEYCOLS_LI, KEYTAB_LI
        )

    ss = gc.open_by_key(spreadsheet_id)
    ws = get_ws(ss, tab)
    if ws is None:
        print(f"[{flow}] {spreadsheet_id}: tab '{tab}' not found — skipping.")
        return 0, 0

    keys_ws = get_or_create_keys_tab(ss, keytab)
    seen = load_keys_tail(keys_ws, KEYS_TAIL)

    # Tail window bounds
    max_row = ws.row_count
    if max_row < start_row:
        return 0, 0
    window_start = max(start_row, max_row - TAIL_WINDOW_ROWS + 1)
    max_col = min(highest_needed_col(mapping, required), ws.col_count)

    total_appended = 0
    total_scanned  = 0
    batch_rows: List[List[str]] = []
    new_keys_buf: List[str] = []

    r = window_start
    while r <= max_row:
        page_end = min(r + PAGE_ROWS - 1, max_row)
        rng = f"{gspread.utils.rowcol_to_a1(r,1)}:{gspread.utils.rowcol_to_a1(page_end,max_col)}"
        values = values_get_safe(ws, rng)

        for i, row in enumerate(values):
            abs_row = r + i
            total_scanned += 1

            # Validate
            good = True
            for idx in required:
                if (idx - 1) >= len(row) or str(row[idx - 1]).strip() == "":
                    good = False
                    break
            if not good:
                continue

            k = make_composite_key(row, keycols)
            if k in seen:
                continue

            if START_FROM_NOW:
                # Baseline: store key only
                new_keys_buf.append(k)
                seen.add(k)
                if len(new_keys_buf) >= BATCH_APPEND_ROWS:
                    append_keys_and_update_marker(keys_ws, new_keys_buf)
                    new_keys_buf.clear()
                continue

            # Normal: append + remember key
            out = map_row_to_master(row, mapping, statics, master_width)
            batch_rows.append(out)
            new_keys_buf.append(k)
            seen.add(k)

            if len(batch_rows) >= BATCH_APPEND_ROWS:
                append_rows_safe(master_tickets, batch_rows, "RAW")
                append_keys_and_update_marker(keys_ws, new_keys_buf)
                total_appended += len(batch_rows)
                batch_rows.clear()
                new_keys_buf.clear()

        r = page_end + 1

    # Flush remaining
    if START_FROM_NOW:
        if new_keys_buf:
            append_keys_and_update_marker(keys_ws, new_keys_buf)
    else:
        if batch_rows:
            append_rows_safe(master_tickets, batch_rows, "RAW")
            append_keys_and_update_marker(keys_ws, new_keys_buf)
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
        print("No sources in Source!B2:B — nothing to do.")
        return

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
