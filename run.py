#!/usr/bin/env python3
# run.py — weekly YouTube-views updater
# ------------------------------------
# • Writes ONE new “Views YYYY-MM-DD HH:MM” column per worksheet, no matter
#   how many video IDs are present.
# • Logs total API calls.
# • Prints a final success / error summary.

import os
import time
import json
import pathlib
from datetime import datetime, timezone, timedelta

import nest_asyncio
nest_asyncio.apply()

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
import gspread
import gspread.utils as gsu
from tqdm import tqdm, tqdm as progress


# ──────────────────────────────────────────────────────────────────────────────
# 1. CONSTANTS & HELPERS
# ──────────────────────────────────────────────────────────────────────────────
IST = timezone(timedelta(hours=5, minutes=30))      # India Standard Time

def make_header() -> str:
    """Return a timestamped header in IST, e.g. ‘Views 2025-08-06 01:00’."""
    return datetime.now(IST).strftime("Views %Y-%m-%d %H:%M")


# ──────────────────────────────────────────────────────────────────────────────
# 2. ENV VARS
# ──────────────────────────────────────────────────────────────────────────────
SPREADSHEET_ID = os.environ["SPREADSHEET_ID"]
YT_API_KEY     = os.environ["YT_API_KEY"]
SA_JSON_PATH   = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]


# ──────────────────────────────────────────────────────────────────────────────
# 3. AUTH
# ──────────────────────────────────────────────────────────────────────────────
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


# ──────────────────────────────────────────────────────────────────────────────
# 4. CONFIG
# ──────────────────────────────────────────────────────────────────────────────
HEADER_ROW      = 1      # titles row
ID_COL          = 2      # column B
NEW_COL_FIXED   = None   # set to 3 to always overwrite column C instead

YT_BATCH_SIZE   = 50     # YouTube API allows ≤ 50 ids per call
YT_QPS          = 9      # self-throttle to 9 calls / second

total_yt_calls    = 0    # diagnostics
total_sheet_calls = 0
errors            = []   # per-sheet error log


# ──────────────────────────────────────────────────────────────────────────────
# 5. FUNCTIONS
# ──────────────────────────────────────────────────────────────────────────────
def fetch_views(ids: list[str]) -> dict[str, int]:
    """Return {video_id: viewCount} for ≤ 50 ids (costs 1 quota unit)."""
    resp = yt.videos().list(id=",".join(ids), part="statistics").execute()
    return {item["id"]: int(item["statistics"]["viewCount"])
            for item in resp.get("items", [])}


# ──────────────────────────────────────────────────────────────────────────────
# 6. MAIN LOOP
# ──────────────────────────────────────────────────────────────────────────────
for ws in ss.worksheets():
    print(f"\n▶  {ws.title}")
    try:
        # --- Read full ID column (B) -----------------------------------------
        col = ws.col_values(ID_COL)                 # 1 Sheets API call
        ids = [v.strip() for v in col[HEADER_ROW:] if v.strip()]
        if not ids:
            print("   (no IDs)")
            continue

        # --- Decide where the new column goes (once per sheet) --------------
        dest_col = NEW_COL_FIXED or len(ws.row_values(HEADER_ROW)) + 1

        # --- Fetch view counts ----------------------------------------------
        view_map: dict[str, int] = {}
        for idx, start in enumerate(range(0, len(ids), YT_BATCH_SIZE)):
            chunk = ids[start:start + YT_BATCH_SIZE]
            view_map.update(fetch_views(chunk))
            total_yt_calls += 1

            # self-throttle every YT_QPS calls
            if (idx + 1) % YT_QPS == 0:
                time.sleep(1.1)

        # --- Build output values --------------------------------------------
        col_len   = len(col)                         # includes header + blanks
        data_rows = col_len - HEADER_ROW             # rows below header
        top_left  = gsu.rowcol_to_a1(HEADER_ROW, dest_col)
        bottom    = gsu.rowcol_to_a1(HEADER_ROW + data_rows, dest_col)
        rng_out   = f"{top_left}:{bottom}"

        values = [[make_header()]]                   # header cell
        for v in col[HEADER_ROW:]:                   # preserve row alignment
            vid = v.strip()
            values.append([view_map.get(vid, "") if vid else ""])

        # --- Write to Sheets (1 call per worksheet) --------------------------
        ws.batch_update([{"range": rng_out, "values": values}])
        total_sheet_calls += 1

        col_letter = ''.join(filter(str.isalpha, gsu.rowcol_to_a1(1, dest_col)))
        print(f"   ✔ {len(ids):>4} IDs written to column {col_letter}")

    except Exception as exc:
        # Log and continue with the next worksheet
        msg = f"{ws.title}: {exc}"
        errors.append(msg)
        print(f"   ✖ ERROR — {msg}")


# ──────────────────────────────────────────────────────────────────────────────
# 7. SUMMARY
# ──────────────────────────────────────────────────────────────────────────────
print("\n──────────────────────────────────────────────────────────────────────────")
print(f"YouTube API calls : {total_yt_calls}")
print(f"Sheets API calls  : {total_sheet_calls}")

if errors:
    print("\nFinished with errors:")
    for e in errors:
        print(f" • {e}")
    # non-zero exit code helps CI workflows fail loudly
    raise SystemExit(1)
else:
    print("\nAll sheets updated successfully ✅")
