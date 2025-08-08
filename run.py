#!/usr/bin/env python3
# run.py — weekly YouTube-views updater
# ------------------------------------
# • ONE new “Views YYYY-MM-DD HH:MM” column per worksheet.
# • Auto-expands sheet width if needed.
# • Read-quota back-off + per-sheet throttle.
# • Final success/error summary.

import os
import time
from datetime import datetime, timezone, timedelta

import nest_asyncio
nest_asyncio.apply()

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import gspread
import gspread.utils as gsu


# ──────────────────────────────────────────────────────────────────────────────
# 1. CONSTANTS & HELPERS
# ──────────────────────────────────────────────────────────────────────────────
IST = timezone(timedelta(hours=5, minutes=30))          # India Standard Time
HEADER_ROW        = 1
ID_COL            = 2                                   # column B
NEW_COL_FIXED     = None                                # or 3 to always use C
YT_BATCH_SIZE     = 50
YT_QPS            = 9
SHEET_DELAY_SECS  = 1.5                                 # throttle Sheets reads
MAX_READ_RETRIES  = 3                                   # for 429s

def make_header() -> str:
    return datetime.now(IST).strftime("Views %Y-%m-%d %H:%M")


# ──────────────────────────────────────────────────────────────────────────────
# 2. ENV & AUTH
# ──────────────────────────────────────────────────────────────────────────────
SPREADSHEET_ID = os.environ["SPREADSHEET_ID"]
YT_API_KEY     = os.environ["YT_API_KEY"]
SA_JSON_PATH   = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]

creds = Credentials.from_service_account_file(
    SA_JSON_PATH,
    scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ],
)
gc  = gspread.authorize(creds)
ss  = gc.open_by_key(SPREADSHEET_ID)
yt  = build("youtube", "v3", developerKey=YT_API_KEY)


# ──────────────────────────────────────────────────────────────────────────────
# 3. UTILS
# ──────────────────────────────────────────────────────────────────────────────
def yt_fetch_views(ids: list[str]) -> dict[str, int]:
    """Return {video_id: viewCount} for ≤ 50 ids (costs 1 quota unit)."""
    resp = yt.videos().list(id=",".join(ids), part="statistics").execute()
    return {item["id"]: int(item["statistics"]["viewCount"])
            for item in resp.get("items", [])}

def safe_col_values(ws: gspread.Worksheet, col_index: int) -> list[str]:
    """Read a column with exponential back-off on 429 quota errors."""
    for attempt in range(1, MAX_READ_RETRIES + 1):
        try:
            return ws.col_values(col_index)            # one read call
        except HttpError as err:
            if err.resp.status == 429 and attempt < MAX_READ_RETRIES:
                wait = 30 * attempt
                print(f"   ⏳ Hit Sheets read-quota (429). Sleeping {wait}s …")
                time.sleep(wait)
            else:
                raise


# ──────────────────────────────────────────────────────────────────────────────
# 4. MAIN
# ──────────────────────────────────────────────────────────────────────────────
total_yt_calls    = 0
total_sheet_calls = 0
errors            = []

for ws in ss.worksheets():
    print(f"\n▶  {ws.title}")
    try:
        # --- read IDs (one read call, retriable) -----------------------------
        col = safe_col_values(ws, ID_COL)
        ids = [v.strip() for v in col[HEADER_ROW:] if v.strip()]
        if not ids:
            print("   (no IDs)")
            time.sleep(SHEET_DELAY_SECS)
            continue

        # --- choose destination column --------------------------------------
        dest_col = NEW_COL_FIXED or ws.col_count + 1       # only metadata read
        if dest_col > ws.col_count:                        # expand if necessary
            ws.add_cols(dest_col - ws.col_count)

        # --- gather view counts (may need several API calls) -----------------
        view_map = {}
        for idx, start in enumerate(range(0, len(ids), YT_BATCH_SIZE)):
            chunk = ids[start : start + YT_BATCH_SIZE]
            view_map.update(yt_fetch_views(chunk))
            total_yt_calls += 1
            if (idx + 1) % YT_QPS == 0:
                time.sleep(1.1)

        # --- build output column --------------------------------------------
        col_len   = len(col)                       # existing rows in ID col
        data_rows = col_len - HEADER_ROW
        rng_out   = f"{gsu.rowcol_to_a1(HEADER_ROW, dest_col)}:" \
                    f"{gsu.rowcol_to_a1(HEADER_ROW + data_rows, dest_col)}"

        values = [[make_header()]]                 # header
        for v in col[HEADER_ROW:]:
            vid = v.strip()
            values.append([view_map.get(vid, "") if vid else ""])

        # --- write (one Sheets call) ----------------------------------------
        ws.batch_update([{"range": rng_out, "values": values}])
        total_sheet_calls += 1

        col_letter = gsu.rowcol_to_a1(1, dest_col).rstrip("1")
        print(f"   ✔ {len(ids):>4} IDs written to column {col_letter}")

    except Exception as exc:
        msg = f"{ws.title}: {exc}"
        errors.append(msg)
        print(f"   ✖ ERROR — {msg}")

    # --- honor per-sheet throttle -------------------------------------------
    time.sleep(SHEET_DELAY_SECS)


# ──────────────────────────────────────────────────────────────────────────────
# 5. SUMMARY
# ──────────────────────────────────────────────────────────────────────────────
print("\n──────────────────────────────────────────────────────────────────────────")
print(f"YouTube API calls : {total_yt_calls}")
print(f"Sheets API calls  : {total_sheet_calls}")

if errors:
    print("\nFinished with errors:")
    for e in errors:
        print(f" • {e}")
    raise SystemExit(1)
else:
    print("\nAll sheets updated successfully ✅")
