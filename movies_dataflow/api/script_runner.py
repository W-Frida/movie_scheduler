import threading, time, shutil, datetime, requests, logging, json, os, sys, gspread
from dotenv import load_dotenv
from subprocess import Popen, PIPE
from moviescraper.utils.data_merger import merge_cleaned_outputs
from oauth2client.service_account import ServiceAccountCredentials

SPIDER_BATCHES = [["amba", "vs"], ["venice", "sk", "showtimes"]]

# ✅ 環境變數載入
load_dotenv()
BASE_URL = os.getenv("BASE_URL", "http://localhost:8000")
UPLOAD_URL = f"{BASE_URL}/upload"
CREDENTIALS_PATH = os.getenv("CREDENTIALS_PATH", "/etc/secrets/credentials.json")
SPREADSHEET_NAME = os.getenv("SPREADSHEET_NAME")

def get_spreadsheet():
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_PATH, scope)
    return gspread.authorize(creds).open(SPREADSHEET_NAME)

def clean_data_folder():
    if os.path.exists("data"):
        shutil.rmtree("data")
    os.makedirs("data")

def run_batch_script_with_ping(script: str, ping_url: str):
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

    all_stdout, all_stderr = "", ""
    returncode = 0

    try:
        logging.info("🧹 清除並建立 data 資料夾")
        clean_data_folder()

        for batch in SPIDER_BATCHES:
            target_arg = "--targets=" + ",".join(batch)
            proc = Popen([sys.executable, script, "--mode=subprocess", target_arg], stdout=PIPE, stderr=PIPE, text=True)

            try:
                stdout, stderr = proc.communicate(timeout=600)
            except Exception:
                proc.kill()
                stdout, stderr = proc.communicate()
                returncode = -1
                logging.error("⏱️ Subprocess timeout，已強制終止")

            all_stdout += stdout
            all_stderr += stderr
            logging.info(f"✅ 批次完成: {batch}")
            logging.info(f"📤 STDOUT:\n{stdout}")
            logging.warning(f"⚠️ STDERR:\n{stderr}")

        # 最後合併並上傳
        if any(f.endswith("_formated.json") for f in os.listdir("data")):
            logging.info("📦 合併所有影城資料 → 匯出 all_cleaned.json")
            merge_cleaned_outputs("data", "*_formated.json", "all_cleaned.json")

            try:
                with open("data/all_cleaned.json", encoding="utf-8") as f:
                    items = json.load(f)
                r = requests.post(UPLOAD_URL, json=items)
                if r.status_code != 200:
                    logging.warning(f"❌ 上傳失敗：{r.status_code} → {r.text}")
                else:
                    logging.info(f"✅ 上傳成功：{r.status_code} → {r.text}")
            except Exception as e:
                logging.warning(f"❌ 上傳失敗：{e}")
        else:
            logging.warning("⚠️ 無可合併資料，跳過上傳")


    finally:
        stop_event.set()
        ping_thread.join()
        end_ts = datetime.datetime.now().isoformat()
        trace_to_sheet(script, start_ts, end_ts, all_stdout, all_stderr, returncode, ping_stats)

def trace_to_sheet(script_name: str, start_ts: str, end_ts: str, stdout: str, stderr: str, returncode: int, ping_stats: dict = None):
    try:
        sheet = get_spreadsheet()
        ws = sheet.worksheet("log")
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
