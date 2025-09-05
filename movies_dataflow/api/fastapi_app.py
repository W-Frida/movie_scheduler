from dotenv import load_dotenv
from api.script_runner import run_batch_script_with_ping
import datetime, os, gspread, logging
from fastapi import FastAPI, HTTPException, Request, BackgroundTasks
from pydantic import BaseModel
from typing import List
from oauth2client.service_account import ServiceAccountCredentials
from subprocess import PIPE

logging.basicConfig(level=logging.INFO)

load_dotenv()
CREDENTIALS_PATH = os.getenv("CREDENTIALS_PATH", "/etc/secrets/credentials.json")
SPREADSHEET_NAME = os.getenv("SPREADSHEET_NAME")

app = FastAPI()

def get_spreadsheet():
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_PATH, scope)
    return gspread.authorize(creds).open(SPREADSHEET_NAME)

class MovieItem(BaseModel):
    電影名稱: str
    影院: str
    放映版本: str
    日期: str
    時刻表: List[str]
    city: str
    cinema: str
    網址: str
    地址: str

    class Config:
        extra = "forbid"  # 🚫 禁止出現未定義欄位

@app.get("/")
def home():
    return {"message": "FastAPI is running!"}

# 確認服務是否在線
@app.get("/healthz")
def health_check():
    return {"status": "ok", "timestamp": datetime.datetime.now().isoformat()}

# 傳送資料到 google sheet 儲存
@app.post("/upload")
def upload_data(items: List[MovieItem]):
    spreadsheet = get_spreadsheet()
    worksheet = rotate_movies_worksheet(spreadsheet)  # 每次重命名、刪除、建立分頁
    rows = prepare_rows(items)
    result = write_rows(rows, worksheet)
    return result

# webhook 入口
@app.post("/trigger-update")
def trigger_update(request: Request, background_tasks: BackgroundTasks):
    api_key = request.headers.get("x-api-key") or request.headers.get("X-Api-Key")
    if not api_key or api_key != os.getenv("UPDATER_API_KEY"):
        raise HTTPException(status_code=403, detail="Invalid API key")

    background_tasks.add_task(run_batch_script_with_ping, "spider_executor.py", "https://movies-fastapi-9840.onrender.com/healthz")
    return {"status": "started"}  # ⏱ 即時回應

@app.post("/trigger-direct-update")
def trigger_direct_update(request: Request, background_tasks: BackgroundTasks):
    api_key = request.headers.get("x-api-key") or request.headers.get("X-Api-Key")
    if not api_key or api_key != os.getenv("UPDATER_API_KEY"):
        raise HTTPException(status_code=403, detail="Invalid API key")

    # ✅ 直接執行 auto_updater.py，不啟用 ping loop
    background_tasks.add_task(run_direct_updater)
    return {"status": "started_direct"}  # ⏱ 即時回應

def run_direct_updater():
    import sys
    from subprocess import Popen, PIPE

    logging.info("🚀 直接執行 auto_updater.py（無 ping）")
    proc = Popen([sys.executable, "auto_updater.py"], stdout=PIPE, stderr=PIPE, text=True)
    stdout, stderr = proc.communicate()

    logging.info(f"📤 STDOUT:\n{stdout}")
    logging.warning(f"⚠️ STDERR:\n{stderr}")
# -------------------------------------------------------------
# google sheet
# -------------------------------------------------------------
def prepare_rows(items: list) -> list[list[str]]:
    rows = []
    for item in items:
        try:
            row = [
                item.city.strip(),
                item.cinema.strip(),
                item.影院.strip(),
                item.日期.strip(),
                item.電影名稱.strip(),
                item.放映版本.strip(),
                ", ".join([t.strip() for t in item.時刻表]),
                item.網址.strip(),
                item.地址.strip()
            ]
            rows.append(row)
        except Exception as e:
            print(f'❌ 清洗失敗：{e}')
    return rows

def write_rows(rows: list[list[str]], worksheet) -> dict:
    try:
        # 🧩 定義欄位標頭
        header = ["地區", "cinema", "影院", "日期", "電影名稱", "放映版本", "時刻表", "網址", "地址"]
        worksheet.update("A1:I1", [header])

        # 📦 寫入資料從第 2 列開始（根據 rows 長度計算）
        worksheet.update(f"A2:I{len(rows)+1}", rows)

        return {"status": "success", "count": len(rows)}

    except Exception as e:
        print(f"❌ 寫入失敗：{e}")
        return {"status": "error", "message": str(e)}


# 分頁複製、重命名、清空movies
def rotate_movies_worksheet(spreadsheet):
    try:
        try:
            old_backup = spreadsheet.worksheet("pre_movies")
            logging.info("🧹 移除 pre_movies 分頁")
            spreadsheet.del_worksheet(old_backup)
        except Exception as e:
            pass

        movies_ws = spreadsheet.worksheet("movies")

        logging.info("📦 將 movies 分頁複製並命名為 pre_movies")
        backup_ws = spreadsheet.duplicate_sheet(movies_ws.id)
        backup_ws.update_title("pre_movies")

        logging.info("✅ movies 清空")
        movies_ws.clear()

        return movies_ws

    except Exception as e:
        print(f"❌ 分頁備份與清空失敗：{e}")
        raise

