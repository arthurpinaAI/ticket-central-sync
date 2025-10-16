# Ticket Central Sync (GitHub Actions + gspread)

## What it does
- Reads Google Sheet IDs/URLs from **Source!B2:B** in your master workbook.
- For each source, pulls:
  - **ALL TICKETS (LIVE)** (data from row 4)
  - **LINKEDIN VIEWS (LIVE)** (data from row 3)
- Validity rules:
  - **ALL**: B and C non-empty
  - **LI**: B, C, D non-empty
- Mappings:
  - **ALL → Tickets**: A→A(1), C→B(2), J→E(5), B→F(6), K→G(7), L→H(8), D→P(16)
  - **LI → Tickets**: A→A(1), B→F(6), C→B(2), E→H(8), D→C(3), plus statics E=“LinkedIn - LX”, G=“DD”
- Appends to **Tickets** after the last row, padding rows to consistent width.
- Saves per-source cursors in **__Markers** so runs are resumable and dedupe-safe.

## Setup
1. Create a Google Cloud Service Account (Sheets + Drive scopes).
2. Enable Sheets & Drive APIs.
3. Share **every source sheet** and your **master** with the service account email (Editor).
4. Add GitHub **Secrets**:
   - `GOOGLE_SERVICE_ACCOUNT_JSON` — entire JSON of your service account.
   - `MASTER_SPREADSHEET_ID` — the ID of your master workbook.
5. (Optional) Add GitHub **Variables** (Repository → Settings → Variables):
   - `MASTER_TICKETS_TAB` (default `Tickets`)
   - `MASTER_SOURCE_TAB`  (default `Source`)
   - `MARKERS_TAB_NAME`   (default `__Markers`)
6. Push this repo. Open **Actions** tab and run **“Ticket Central Sync”** with *Workflow dispatch*.

## “Start from now”
If you want to skip all historical rows the first time a given source is seen:
- Run the workflow with `initFromNow=true`
- Or set repository variable `INIT_FROM_NOW=true` (not required; the workflow input is enough)

## Operation
- The workflow runs every 10 minutes and in small chunks (configurable).
- Cursors ensure no duplicates and safe resumption.
- You can increase `CHUNK_ROWS` if you want faster catch-up.

