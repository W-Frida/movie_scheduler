from dotenv import load_dotenv
import datetime, re, os, subprocess, gspread
from fastapi import FastAPI, Header, HTTPException
from auto_updater import main as run_updater
from pydantic import BaseModel
from typing import List
from oauth2client.service_account import ServiceAccountCredentials

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


@app.post("/upload")
def upload_data(items: List[MovieItem]):
    spreadsheet = get_spreadsheet()
    worksheet = rotate_movies_worksheet(spreadsheet)  # 每次重命名、刪除、建立分頁
    rows = prepare_rows(items)
    result = write_rows(rows, worksheet)
    return result

@app.post("/trigger-update")
def trigger_update(x_api_key: str = Header(default=None)):
    if not x_api_key or x_api_key != os.getenv("UPDATER_API_KEY"):
        raise HTTPException(status_code=403, detail="Invalid API key")

    try:
        run_updater() # 🚀 直接觸發 auto_updater 的 main()
        return {"status": "success"}
    except Exception as e:
        return {"status": "error", "message": str(e)}


@app.get("/")
def home():
    return {"message": "FastAPI is running!"}

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
                item.網址.strip()
            ]
            rows.append(row)
        except Exception as e:
            print(f'❌ 清洗失敗：{e}')
    return rows

def write_rows(rows: list[list[str]], worksheet) -> dict:
    try:
        # 🧩 定義欄位標頭
        header = ["地區", "影城", "影院", "日期", "電影名稱", "放映版本", "時刻表", "網址"]
        worksheet.update("A1:H1", [header])

        # 📦 寫入資料從第 2 列開始（根據 rows 長度計算）
        worksheet.update(f"A2:H{len(rows)+1}", rows)

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
            if len(row) > date_index:
                raw = row[date_index].strip().replace("/", "-")
                cleaned = re.sub(r"[\(（][一二三四五六日天][\)）]", "", raw)

                try:
                    dt = datetime.datetime.strptime(cleaned, "%Y-%m-%d")
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
        elif is_date_title(ws.title):
            try:
                date = datetime.datetime.strptime(ws.title.strip(), "%Y-%m-%d")
                dated.append((ws, date))
            except:
                continue

    # 按日期排序，保留最新 keep_latest 個分頁
    for ws, _ in sorted(dated, key=lambda x: x[1])[:-keep_latest]:
        spreadsheet.del_worksheet(ws)

    # 🆕 新建最新分頁名稱為 "movies"
    new_ws = spreadsheet.add_worksheet(title="movies", rows="30000", cols="26")
    return new_ws

def is_date_title(title: str) -> bool:
    return bool(re.match(r"^\d{4}-\d{2}-\d{2}$", title.strip()))
