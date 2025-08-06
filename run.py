# run.py  – weekly YouTube-views updater
import os, json, time, pathlib
import nest_asyncio
nest_asyncio.apply()

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
import gspread, gspread.utils as gsu
from tqdm import tqdm
from datetime import datetime, timezone, timedelta

# IST is UTC+5:30
IST = timezone(timedelta(hours=5, minutes=30))

def make_header():
    """Return e.g. 'Views 2025-08-06 01:00' in IST."""
    return datetime.now(IST).strftime("Views %Y-%m-%d %H:%M")

# ========== 1. Read env vars ==========
SPREADSHEET_ID = os.environ["SPREADSHEET_ID"]
YT_API_KEY     = os.environ["YT_API_KEY"]
SA_JSON_PATH   = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]

# ========== 2. Auth ==========
creds = Credentials.from_service_account_file(
    SA_JSON_PATH,
    scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ],
)
gc  = gspread.authorize(creds)
ss  = gc.open_by_key(SPREADSHEET_ID)

yt = build("youtube", "v3", developerKey=YT_API_KEY)

def fetch_views(ids):
    """Return {id:viewCount} for up to 50 ids (1 quota unit)."""
    resp = yt.videos().list(id=",".join(ids), part="statistics").execute()
    return {it["id"]: int(it["statistics"]["viewCount"]) for it in resp.get("items", [])}

# ========== 3. Config ==========
HEADER_ROW      = 1      # titles row
ID_COL          = 2      # column B
NEW_COL_FIXED   = None   # or 3 to always use column C

YT_BATCH_SIZE   = 50
YT_QPS          = 9

total_yt, total_sheet = 0, 0

# track how many API calls we make
total_sheet_calls = 0
total_yt_calls    = 0

# ========== 4. Loop sheets ==========
for ws in ss.worksheets():
    print(f"\n▶  {ws.title}")

    # --- read IDs ---
    col = ws.col_values(ID_COL)               # one API call
    ids = [v.strip() for v in col[HEADER_ROW:] if v.strip()]
    total_sheet += 1
    if not ids:
        print("   (no IDs)")
        continue

    # --- fetch views ---
    view_map = {}
    for idx, start in enumerate(range(0, len(ids), YT_BATCH_SIZE)):
        chunk = ids[start : start + YT_BATCH_SIZE]
        view_map.update(fetch_views(chunk))
        total_yt += 1
        if (idx + 1) % YT_QPS == 0:
            time.sleep(1.1)

        # ----- WRITE results  (1 Sheets call) -----
        # 1) How many physical rows exist in the column (incl. blanks)?
        col_len   = len(col)                         # col came from ws.col_values()
        data_rows = col_len - HEADER_ROW             # rows below the header
        
        dest_col  = NEW_COL_FIXED or len(ws.row_values(HEADER_ROW)) + 1
        
        top_left  = gsu.rowcol_to_a1(HEADER_ROW, dest_col)
        bottom    = gsu.rowcol_to_a1(HEADER_ROW + data_rows, dest_col)
        rng_out   = f"{top_left}:{bottom}"
        
        # 2) Build a value for *every* row, preserving gaps
        values = [[make_header()]]                    # header cell
        for v in col[HEADER_ROW:]:                   # original column slice (incl. '')
            vid = v.strip()
            values.append([view_map.get(vid, "") if vid else ""])
        
        ws.batch_update([{"range": rng_out, "values": values}])
        total_sheet_calls += 1
        
        col_letter = ''.join(filter(str.isalpha, gsu.rowcol_to_a1(1, dest_col)))
        print(f"   ✔ {len(ids):>4} IDs written to column {col_letter}")


print(f"\nYouTube calls : {total_yt}")
print(f"Sheets calls  : {total_sheet}")
print("Done ✅")
