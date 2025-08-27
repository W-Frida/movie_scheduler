from dotenv import load_dotenv
from api.script_runner import run_batch_script_with_ping
import datetime, re, os, subprocess, gspread, logging, threading, time, requests
from fastapi import FastAPI, HTTPException, Request, BackgroundTasks
from pydantic import BaseModel
from typing import List
from oauth2client.service_account import ServiceAccountCredentials
from subprocess import PIPE, Popen

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
    影城: str
    放映版本: str
    日期: str
    時刻表: List[str]
    city: str
    cinema: str
    網址: str
    地址: str

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

# --------------------------------------------------------------------------------------------
# 在背景任務期間 ping Web Service 防止 Render 休眠
# --------------------------------------------------------------------------------------------
def run_script_with_ping(script: str, ping_url: str):
    stop_event = threading.Event()
    ping_stats = {"success": 0, "fail": 0}
    start_ts = datetime.datetime.now().isoformat()

    def ping_loop(url: str):
        while not stop_event.is_set():
            try:
                r = requests.get(url, timeout=5)
                ping_stats["success"] += 1
                logging.info(f"🌐 Ping {url} - {r.status_code}")
            except Exception as e:
                ping_stats["fail"] += 1
                logging.warning(f"⚠️ Ping failed: {e}")
            time.sleep(120)

    ping_thread = threading.Thread(target=ping_loop, args=(ping_url,))
    ping_thread.start()

    try:
        proc = Popen(["python", script, "--subprocess"], stdout=PIPE, stderr=PIPE, text=True)
        try:
            stdout, stderr = proc.communicate(timeout=1200)
        except subprocess.TimeoutExpired:
            proc.kill()
            stdout, stderr = proc.communicate()
            returncode = -1
            logging.error("⏱️ Subprocess timeout，已強制終止")
        returncode = proc.returncode
        logging.info(f"📤 STDOUT:\n{stdout}")
        logging.warning(f"⚠️ STDERR:\n{stderr}")
        logging.info(f"🔚 Return code: {returncode}")
    finally:
        stop_event.set()
        ping_thread.join()
        end_ts = datetime.datetime.now().isoformat()
        trace_to_sheet(script, start_ts, end_ts, stdout, stderr, returncode, ping_stats)

def trace_to_sheet(script_name: str, start_ts: str, end_ts: str, stdout: str, stderr: str, returncode: int, ping_stats: dict = None):
    try:
        sheet = get_spreadsheet()
        ws = sheet.worksheet("log")  # 確保已建立分頁
        row = [
            start_ts,
            end_ts,
            script_name,
            returncode,
            f"✅ {ping_stats['success']} / ❌ {ping_stats['fail']}" if ping_stats else "",
            stdout[:300],
            stderr[:300]
        ]
        ws.append_row(row)
    except Exception as e:
        logging.error(f"❌ trace_to_sheet 寫入失敗：{e}")

# -------------------------------------------------------------
def prepare_rows(items: list) -> list[list[str]]:
    rows = []
    for item in items:
        try:
            row = [
                item.city.strip(),
                item.cinema.strip(),
                item.影城.strip(),
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
        header = ["地區", "影城", "影院", "日期", "電影名稱", "放映版本", "時刻表", "網址", "地址"]
        worksheet.update("A1:I1", [header])

        # 📦 寫入資料從第 2 列開始（根據 rows 長度計算）
        worksheet.update(f"A2:I{len(rows)+1}", rows)

        return {"status": "success", "count": len(rows)}

    except Exception as e:
        print(f"❌ 寫入失敗：{e}")
        return {"status": "error", "message": str(e)}

# 動態變更原 movies 表單為日期命名
def infer_previous_date(worksheet) -> str:
    try:
        rows = worksheet.get_all_values()
        if not rows:
            return "unknown"

        try:
            date_index = rows[0].index("日期")
        except ValueError:
            return "unknown"

        date_candidates = []
        for row in rows[1:]:
            # if len(row) > date_index:
            date_str = row[date_index].strip().replace("/", "-")
            try:
                dt = datetime.datetime.strptime(date_str, "%Y-%m-%d")
                date_candidates.append(dt)
            except:
                continue

        if date_candidates:
            return min(date_candidates).strftime("%Y-%m-%d")

    except Exception as e:
        print(f"❌ 分頁日期推斷失敗：{e}")

    return "unknown"

# 分頁輪替、命名、清理、防爆炸
def rotate_movies_worksheet(spreadsheet, keep_latest=2):
    # 取得所有分頁並解析日期
    sheets = spreadsheet.worksheets()
    dated = []
    for ws in sheets:
        if ws.title == "movies":
            last_date = infer_previous_date(ws)
            ws.update_title(last_date)
        elif ws.title == "unknown":
            spreadsheet.del_worksheet(ws)
        elif is_date_title(ws.title):
            try:
                date = datetime.datetime.strptime(ws.title.strip(), "%Y-%m-%d")
                dated.append((ws, date))
            except:
                continue

    # 按日期排序，保留最新 keep_latest 個分頁
    for ws, _ in sorted(dated, key=lambda x: x[1])[:-keep_latest]:
        try:
            logging.info(f"🧹 移除分頁：{ws.title}")
            spreadsheet.del_worksheet(ws)
        except Exception as e:
            logging.warning(f"❌ 分頁刪除失敗：{ws.title} → {e}")

    # 🆕 新建最新分頁名稱為 "movies"
    new_ws = spreadsheet.add_worksheet(title="movies", rows="30000", cols="26")
    return new_ws

def is_date_title(title: str) -> bool:
    return bool(re.match(r"^\d{4}-\d{2}-\d{2}$", title.strip()))
