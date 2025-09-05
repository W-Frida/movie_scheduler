import threading, time, shutil, datetime, requests, logging, json, os, sys, gspread
from dotenv import load_dotenv
from subprocess import Popen, PIPE
from moviescraper.utils.data_merger import merge_cleaned_outputs
from oauth2client.service_account import ServiceAccountCredentials

# SPIDER_BATCHES = [["amba", "vs"], ["venice", "sk", "showtimes"]]
SPIDER_LIST = ["vs","venice","sk","showtimes","amba", "sbc"]

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
        logging.info("目前進度: 清除並建立 data 資料夾")
        clean_data_folder()

        for batch in SPIDER_LIST:
            # target_arg = "--targets=" + ",".join(batch)
            proc = Popen([sys.executable, script, "--mode=subprocess", "--targets=" + batch], stdout=PIPE, stderr=PIPE, text=True)
            stdout, stderr = monitor_subprocess(proc, timeout=1800, ping_url=ping_url)

            all_stdout += stdout
            all_stderr += stderr
            logging.info(f"✅ 批次完成: {batch}")
            logging.info(f"📤 STDOUT:\n{stdout}")
            logging.warning(f"⚠️ STDERR:\n{stderr}")

        # 檢查:在合併前列出所有檔案與筆數
        logging.info("📋 合併前檢查 JSON 檔案與筆數：")
        json_summary = []
        for f in os.listdir("data"):
            if f.endswith("_formated.json"):
                path = os.path.join("data", f)
                try:
                    with open(path, encoding="utf-8") as file:
                        items = json.load(file)
                    count = len(items)
                    logging.info(f"📦 {f} → {count} 筆")
                    json_summary.append(f"{f}:{count}")
                except Exception as e:
                    logging.warning(f"⚠️ 無法讀取 {f}：{e}")
                    json_summary.append(f"{f}:讀取失敗")

        # 最後合併並上傳
        for f in os.listdir("data"):
            if f.endswith("_formated.json"):
                logging.info("目前進度: 合併資料 → 匯出 all_cleaned.json")
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

def monitor_subprocess(proc, timeout=1800, ping_url=None):
    start_time = time.time()
    stdout, stderr = "", ""

    while True:
        if proc.poll() is not None:
            break
        elapsed = time.time() - start_time
        if elapsed > timeout:
            proc.kill()
            logging.error("⏱️ Subprocess timeout，已強制終止")
            break
        if int(elapsed) % 120 == 0 and ping_url:
            try:
                r = requests.get(ping_url, timeout=5)
                logging.info(f"🌐 Ping {ping_url} - {r.status_code}")
            except Exception as e:
                logging.warning(f"⚠️ Ping failed: {e}")
        time.sleep(10)

    try:
        stdout, stderr = proc.communicate()
    except Exception as e:
        logging.error(f"❌ communicate() 失敗：{e}")
    return stdout, stderr


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
