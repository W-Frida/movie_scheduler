import os, json, requests, shutil, asyncio, sys
from spider_executor import SpiderExecutor
from dotenv import load_dotenv
from moviescraper.utils.data_merger import merge_cleaned_outputs

# ✅ Windows 特殊處理選擇 AsyncioSelectorReactor → 與 Scrapy 相容
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

load_dotenv()
BASE_URL = os.getenv("BASE_URL", "http://localhost:8000")
UPLOAD_URL = f"{BASE_URL}/upload"
CREDENTIALS_PATH = os.getenv("CREDENTIALS_PATH", "/etc/secrets/credentials.json")
SPREADSHEET_NAME = os.getenv("SPREADSHEET_NAME")

def clean_data_folder():
    if os.path.exists('data'):
        shutil.rmtree('data')
    os.makedirs('data')

def upload_to_fastapi():
    try:
        with open('data/all_cleaned.json', 'r', encoding='utf-8') as f:
            payload = json.load(f)
        res = requests.post(UPLOAD_URL, json=payload)
        res.raise_for_status()
        try:
            result = res.json()
        except Exception:
            print('⚠️ FastAPI 回傳非 JSON，原始內容：', res.text)
            result = {'status': 'error', 'message': res.text.strip()}

        print(f'✅ 傳送成功：{res.status_code} / 共 {len(payload)} 筆 → {result}')
    except Exception as e:
        print(f'❌ 傳送失敗：{e}')

def main():
    clean_data_folder()

    spider_task = SpiderExecutor()
    mode = "cli"
    if "--async" in sys.argv:
        mode = "async"
    elif "--subprocess" in sys.argv:
        mode = "subprocess"

    print(f'目前進度: 啟動 scrapy...模式 : {mode}')
    spider_task.run(mode=mode)

    print(f'目前進度: 合併所有影城資料，匯出 all_cleaned.json')
    merge_cleaned_outputs("data", "*_formated.json", "all_cleaned.json")

    print('目前進度: 傳送資料給 FastAPI /upload...')
    upload_to_fastapi()


if __name__ == '__main__':
    main()
