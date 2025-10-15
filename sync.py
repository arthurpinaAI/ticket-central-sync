#!/usr/bin/env python3
import os, re, json, math, time
from typing import List, Dict, Any, Tuple
from dateutil import tz
import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# --------- Env (required) ----------
MASTER_SPREADSHEET_ID = os.environ["MASTER_SPREADSHEET_ID"]
MASTER_TAB            = os.environ.get("MASTER_TAB", "Tickets")
SOURCE_SPREADSHEET_ID = os.environ["SOURCE_SPREADSHEET_ID"]
SOURCE_TAB            = os.environ.get("SOURCE_TAB", "Source")

# Source tab names in each source spreadsheet
ALL_TAB = os.environ.get("ALL_TAB", "ALL TICKETS (LIVE)")
LI_TAB  = os.environ.get("LI_TAB",  "LINKEDIN VIEWS (LIVE)")

# Chunking / time budget knobs (sane defaults)
CHUNK_SIZE        = int(os.environ.get("CHUNK_SIZE", "20"))   # number of sources per run
SOFT_TIME_LIMIT_S = float(os.environ.get("SOFT_TIME_LIMIT_S", "270"))  # ~4.5 min

# --------- Mappings (1-indexed like your Apps Script) ----------
ALL_HEADER_ROWS = 3  # data starts at row 4
ALL_REQUIRED    = [2, 3]  # B, C required
ALL_MAP = [
  { "src": 1,  "tgt": 1 },  # A → A
  { "src": 3,  "tgt": 2 },  # C → B
  { "src": 10, "tgt": 5 },  # J → E
  { "src": 2,  "tgt": 6 },  # B → F
  { "src": 11, "tgt": 7 },  # K → G
  { "src": 12, "tgt": 8 },  # L → H
  { "src": 4,  "tgt": 16 }  # D → P
]

LI_HEADER_ROWS = 2  # data starts at row 3
LI_REQUIRED    = [2, 3, 4]  # B, C, D
LI_MAP = [
  { "src": 1, "tgt": 1 },                   # A → A
  { "src": 2, "tgt": 6 },                   # B → F
  { "src": 3, "tgt": 2 },                   # C → B
  { "src": 5, "tgt": 8 },                   # E → H
  { "static": "LinkedIn - LX", "tgt": 5 },  # → E
  { "static": "DD",              "tgt": 7 },# → G
  { "src": 4, "tgt": 3 }                    # D → C
]

# --------- Auth ----------
SERVICE_JSON = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.readonly",
]
creds_info = json.loads(SERVICE_JSON)
creds = Credentials.from_service_account_info(creds_info, scopes=SCOPES)
gc = gspread.authorize(creds)
sheets_v4 = build("sheets", "v4", credentials=creds)

# --------- Helpers ----------
SPREADSHEET_ID_RE = re.compile(r"/d/([A-Za-z0-9_-]+)")

def extract_id(url: str) -> str:
    m = SPREADSHEET_ID_RE.search(url)
    if not m:
        raise ValueError(f"Invalid spreadsheet URL: {url}")
    return m.group(1)

def get_or_create_markers_ws(master_id: str):
    sh = gc.open_by_key(master_id)
    try:
        ws = sh.worksheet("__Markers")
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title="__Markers", rows=1000, cols=3)
        # Header
        ws.update("A1:C1", [["key", "value", "updated_at"]])
        # Try to hide (best-effort)
        try:
            ws.hide()
        except Exception:
            pass
    return ws

def read_markers(ws) -> Dict[str, str]:
    records = {}
    vals = ws.get_all_values()
    for i, row in enumerate(vals[1:], start=2):
        if not row or not row[0]:
            continue
        records[row[0]] = row[1] if len(row) > 1 else ""
    return records

def set_markers(ws, updates: Dict[str, str]):
    if not updates:
        return
    # Pull current to know where to write/replace
    vals = ws.get_all_values()
    header = ["key", "value", "updated_at"]
    if not vals:
        ws.update("A1:C1", [header])
        vals = [header]
    key_to_row = {}
    for i, row in enumerate(vals[1:], start=2):
        if row and row[0]:
            key_to_row[row[0]] = i
    rows_to_update = []
    now_str = time.strftime("%Y-%m-%d %H:%M:%S")
    for k, v in updates.items():
        if k in key_to_row:
            r = key_to_row[k]
            rows_to_update.append((r, [k, str(v), now_str]))
        else:
            vals.append([k, str(v), now_str])
    # Batch: update existing
    data = []
    for r, rowvals in rows_to_update:
        data.append({"range": f"__Markers!A{r}:C{r}", "values": [rowvals]})
    if data:
        sheets_v4.spreadsheets().values().batchUpdate(
            spreadsheetId=MASTER_SPREADSHEET_ID,
            body={"valueInputOption": "RAW", "data": data}
        ).execute()
    # Append new
    if len(vals) > 1 + len(key_to_row):
        new_rows = vals[1 + len(key_to_row):]
        sheets_v4.spreadsheets().values().append(
            spreadsheetId=MASTER_SPREADSHEET_ID,
            range="__Markers!A1",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": new_rows},
        ).execute()

def last_row(ws) -> int:
    # gspread get_all_values is reliable for last non-empty row
    return len(ws.get_all_values())

def ensure_master_tab(master_id: str, tab_name: str):
    sh = gc.open_by_key(master_id)
    try:
        return sh.worksheet(tab_name)
    except gspread.exceptions.WorksheetNotFound:
        return sh.add_worksheet(title=tab_name, rows=1000, cols=50)

def build_mapped_row(mapping: List[Dict[str, Any]], src_row: List[Any], width: int) -> List[Any]:
    out = [""] * width
    for m in mapping:
        if "src" in m:
            idx = m["src"] - 1
            out[m["tgt"] - 1] = src_row[idx] if idx < len(src_row) else ""
        else:
            out[m["tgt"] - 1] = m.get("static", "")
    return out

def max_tgt(mapping: List[Dict[str, Any]]) -> int:
    return max(m["tgt"] for m in mapping)

def need_src_cols(mapping: List[Dict[str, Any]], required: List[int]) -> int:
    max_map_src = max([m.get("src", 0) for m in mapping], default=0)
    return max(max_map_src, max(required) if required else 0)

def read_source_urls(index_id: str, tab: str) -> List[str]:
    ws = gc.open_by_key(index_id).worksheet(tab)
    # B2:B (full column)
    colB = ws.col_values(2)  # 1-indexed; col 2 is B
    # Drop header row (B1) implicitly because we start from B2 by ignoring first cell if needed
    # Remove blanks
    return [u for u in colB[1:] if u.strip()]

def fetch_rows(spreadsheet_id: str, tab: str, start_row: int, num_rows: int, num_cols: int) -> List[List[Any]]:
    # Sheets API batchGet is efficient; but we’ll request precise range
    end_row = start_row + num_rows - 1
    rng = f"{tab}!A{start_row}:{chr(64+num_cols)}{end_row}"
    try:
        resp = sheets_v4.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id, range=rng, majorDimension="ROWS"
        ).execute()
        return resp.get("values", [])
    except Exception:
        return []

def append_rows(spreadsheet_id: str, tab: str, rows: List[List[Any]]):
    if not rows:
        return
    sheets_v4.spreadsheets().values().append(
        spreadsheetId=spreadsheet_id,
        range=f"{tab}!A1",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": rows},
    ).execute()

def get_sheet_dims(spreadsheet_id: str, tab: str) -> Tuple[int, int]:
    # Use gspread for simplicity
    sh = gc.open_by_key(spreadsheet_id)
    try:
        ws = sh.worksheet(tab)
    except gspread.exceptions.WorksheetNotFound:
        return (0, 0)
    return (ws.row_count if ws else 0, ws.col_count if ws else 0)

def get_last_used_row(spreadsheet_id: str, tab: str) -> int:
    sh = gc.open_by_key(spreadsheet_id)
    ws = sh.worksheet(tab)
    return last_row(ws)

def process_flow(flow_name: str,
                 src_tab: str,
                 header_rows: int,
                 required_cols: List[int],
                 mapping: List[Dict[str, Any]],
                 marker_prefix: str,
                 master_id: str,
                 master_tab: str,
                 source_urls: List[str],
                 markers_ws) -> Tuple[int, int]:
    """
    Returns: (added_count, processed_sources)
    """
    start_ts = time.time()
    markers = read_markers(markers_ws)
    # Determine master width
    master_ws = ensure_master_tab(master_id, master_tab)
    master_values = master_ws.get_all_values()
    master_width = max(len(master_values[0]) if master_values else 0, max_tgt(mapping))
    need_cols = need_src_cols(mapping, required_cols)

    total_added = 0
    processed_sources = 0

    for url in source_urls:
        if time.time() - start_ts > SOFT_TIME_LIMIT_S:
            break
        sid = extract_id(url)
        key = f"{marker_prefix}{sid}"
        prev = int(markers.get(key, str(header_rows)))
        # Probe source last row & width
        try:
            last_used = get_last_used_row(sid, src_tab)
        except Exception:
            # Tab missing or file not accessible -> skip
            continue
        if last_used <= prev:
            processed_sources += 1
            continue
        rows_to_read = last_used - prev
        # Read in chunks to avoid huge payloads
        BATCH = 2000
        out_rows: List[List[Any]] = []
        cur = 0
        while cur < rows_to_read:
            if time.time() - start_ts > SOFT_TIME_LIMIT_S:
                break
            take = min(BATCH, rows_to_read - cur)
            data = fetch_rows(sid, src_tab, prev + 1 + cur, take, need_cols)
            for i, r in enumerate(data):
                # pad row to need_cols
                padded = r + [""] * max(0, need_cols - len(r))
                if any(not padded[c-1] for c in required_cols):
                    continue
                mapped = build_mapped_row(mapping, padded, master_width)
                out_rows.append(mapped)
            cur += take
        if out_rows:
            append_rows(master_id, master_tab, out_rows)
            total_added += len(out_rows)
            prev = prev + rows_to_read  # marker moves to last row inspected (even if some skipped)
            set_markers(markers_ws, {key: str(prev)})
        processed_sources += 1

    return total_added, processed_sources

def main():
    # Load source URLs
    urls = read_source_urls(SOURCE_SPREADSHEET_ID, SOURCE_TAB)
    # Work in deterministic chunks each run (so long runs split cleanly)
    # We rotate over the list using a simple cursor stored in markers as SYNC_CURSOR_ALL/LI
    markers_ws = get_or_create_markers_ws(MASTER_SPREADSHEET_ID)
    markers = read_markers(markers_ws)

    # Round-robin chunking helper
    def take_chunk(flow_cursor_key: str) -> Tuple[List[str], int]:
        cur = int(markers.get(flow_cursor_key, "0"))
        if cur >= len(urls):
            cur = 0
        end = min(cur + CHUNK_SIZE, len(urls))
        return urls[cur:end], end

    # ---- Flow 1: ALL ----
    all_urls, new_cursor_all = take_chunk("SYNC_CURSOR_ALL")
    added_all, done_all = process_flow(
        flow_name="ALL",
        src_tab=ALL_TAB,
        header_rows=ALL_HEADER_ROWS,
        required_cols=ALL_REQUIRED,
        mapping=ALL_MAP,
        marker_prefix="ALL_",
        master_id=MASTER_SPREADSHEET_ID,
        master_tab=MASTER_TAB,
        source_urls=all_urls,
        markers_ws=markers_ws
    )
    set_markers(markers_ws, {"SYNC_CURSOR_ALL": str(new_cursor_all)})

    # If time remains, run Flow 2
    time_left = SOFT_TIME_LIMIT_S - 10  # guard
    if time.time() - 0 < time_left:
        li_urls, new_cursor_li = take_chunk("SYNC_CURSOR_LI")
        added_li, done_li = process_flow(
            flow_name="LI",
            src_tab=LI_TAB,
            header_rows=LI_HEADER_ROWS,
            required_cols=LI_REQUIRED,
            mapping=LI_MAP,
            marker_prefix="LI_",
            master_id=MASTER_SPREADSHEET_ID,
            master_tab=MASTER_TAB,
            source_urls=li_urls,
            markers_ws=markers_ws
        )
        set_markers(markers_ws, {"SYNC_CURSOR_LI": str(new_cursor_li)})
        print(f"[OK] Added: ALL={added_all}, LI={added_li} | Sources processed: ALL={done_all}, LI={done_li}")
    else:
        print(f"[OK] Added: ALL={added_all} | Sources processed: ALL={done_all} | (LI skipped due to time)")

if __name__ == "__main__":
    main()
