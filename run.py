# ============================================================
# 📦 0. Install required libraries (one-off per runtime)
# ============================================================
!pip install --quiet gspread google-auth google-api-python-client nest_asyncio tqdm

# ============================================================
# 🔐 1. Load Colab secrets ➜ environment variables
#     (run this cell first every session)
# ============================================================
from google.colab import userdata
import os, pathlib, json, nest_asyncio, time
nest_asyncio.apply()
# --- after pulling secrets into os.environ ---
import json, pathlib, os


sa_env = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]

# If the env-var starts with '{', assume it is the JSON text itself
if sa_env.lstrip().startswith('{'):
    json_path = "/content/sa.json"
    pathlib.Path(json_path).write_text(sa_env)
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = json_path
    print("🔄  Wrote service-account JSON to", json_path)


# Mandatory secrets
REQUIRED_SECRETS = ("SPREADSHEET_ID",
                    "YT_API_KEY",
                    "GOOGLE_APPLICATION_CREDENTIALS")

for key in REQUIRED_SECRETS:
    val = os.getenv(key) or userdata.get(key)
    if not val:
        raise ValueError(f"❌ Secret '{key}' not set in Colab ➜ Secrets panel")
    os.environ[key] = val                 # push into env

# OPTIONAL: You may have stored the *entire* service-account JSON
#           in a secret called SA_JSON_CONTENT. If that secret exists,
#           write it to /content/sa.json so the rest of the code can use it.
sa_json_content = None
try:
    sa_json_content = userdata.get("SA_JSON_CONTENT")
except Exception:            # SecretNotFoundError, NotebookAccessError, …
    pass                     # simply means the secret isn't there

if sa_json_content:
    json_path = "/content/sa.json"
    pathlib.Path(json_path).write_text(sa_json_content)
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = json_path


# Show a quick status (doesn't reveal secret values)
print({k: ("set ✅" if os.getenv(k) else "missing ❌") for k in REQUIRED_SECRETS})

# ============================================================
# ⚙️ 2. Configuration
# ============================================================
SPREADSHEET_ID = os.environ["SPREADSHEET_ID"]
YT_API_KEY     = os.environ["YT_API_KEY"]
SA_JSON_PATH   = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]

HEADER_ROW         = 1        # titles row
ID_COL_1BASED      = 2        # column B
NEW_COL_FIXED      = None     # or 3 for always-column-C
HEADER_LABEL       = "Views"  # header to write
YT_BATCH_SIZE      = 50       # max per videos.list call
YT_QPS             = 9        # stay < 10 queries / sec

# ============================================================
# 🔗 3. Authenticate – Google Sheets & YouTube
# ============================================================
from google.oauth2.service_account import Credentials
import gspread, gspread.utils as gsu
from googleapiclient.discovery import build
from tqdm.notebook import tqdm

creds = Credentials.from_service_account_file(
    SA_JSON_PATH,
    scopes=["https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"]
)
gc  = gspread.authorize(creds)
ss  = gc.open_by_key(SPREADSHEET_ID)

yt  = build("youtube", "v3", developerKey=YT_API_KEY)

def fetch_views(ids):
    """Return {video_id: viewCount} for ≤50 ids (costs 1 quota unit)."""
    resp = yt.videos().list(id=",".join(ids), part="statistics").execute()
    return {it["id"]: int(it["statistics"]["viewCount"])
            for it in resp.get("items", [])}

# ============================================================
# 🔁 4. Process every worksheet  (2 Sheets calls each)
# ============================================================
total_yt_calls, total_sheet_calls = 0, 0

for ws in ss.worksheets():
    print(f"\n▶️  Sheet: {ws.title}")

    # ----- READ IDs  (1 Sheets call) -----
    # ----- READ IDs  (1 Sheets API call) -----
    rng = (
        f"{gsu.rowcol_to_a1(HEADER_ROW, ID_COL_1BASED)}:"
        f"{gsu.rowcol_to_a1(ws.row_count, ID_COL_1BASED)}"
    )
    rows = ws.batch_get([rng])[0]                  # list of rows
    ids  = [
        row[0].strip()                            # first cell of the row
        for row in rows[1:]                       # skip header row
        if row and row[0].strip()                 # ignore blank cells
    ]
    total_sheet_calls += 1

    # ----- YOUTUBE fetch -----
    view_map = {}
    for idx, start in enumerate(range(0, len(ids), YT_BATCH_SIZE)):
        chunk = ids[start:start+YT_BATCH_SIZE]
        view_map.update(fetch_views(chunk))
        total_yt_calls += 1
        if (idx+1) % YT_QPS == 0:         # simple QPS throttle
            time.sleep(1.1)

    # ----- WRITE results  (1 Sheets call) -----
    dest_col = NEW_COL_FIXED or len(ws.row_values(HEADER_ROW)) + 1
    top_left = gsu.rowcol_to_a1(HEADER_ROW, dest_col)
    bottom   = gsu.rowcol_to_a1(HEADER_ROW + len(ids), dest_col)
    rng_out  = f"{top_left}:{bottom}"

    values = [[HEADER_LABEL]] + [[view_map.get(v, "")] for v in ids]
    ws.batch_update([{"range": rng_out, "values": values}])
    total_sheet_calls += 1

    # ---------------- status line ----------------
    col_letter = ''.join(filter(str.isalpha, gsu.rowcol_to_a1(1, dest_col)))
    print(f"   ✔  {len(ids):>4} IDs written to column {col_letter}")


# ============================================================
# 📊 5. Quota summary
# ============================================================
print(f"\n🎯  YouTube API calls : {total_yt_calls}  "
      f"(costs {total_yt_calls} quota units)")
print(f"📑  Sheets API calls  : {total_sheet_calls}  "
      f"(≈2 per worksheet)")
print("✅  Done.")
