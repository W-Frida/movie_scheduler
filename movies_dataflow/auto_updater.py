import json, requests, os, shutil, asyncio
from run_spiders import auto_run_spiders
from dotenv import load_dotenv

import asyncio, sys

# ✅ Windows 特殊處理選擇 AsyncioSelectorReactor → 與 Scrapy 相容
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# ✅ 在任何 Twisted import 之前安裝 reactor
import scrapy.utils.reactor
scrapy.utils.reactor.install_reactor("twisted.internet.asyncioreactor.AsyncioSelectorReactor")


load_dotenv()
BASE_URL = os.getenv("BASE_URL", "http://localhost:8000")
UPLOAD_URL = f"{BASE_URL}/upload"
CREDENTIALS_PATH = os.getenv("CREDENTIALS_PATH", "/etc/secrets/credentials.json")
SPREADSHEET_NAME = os.getenv("SPREADSHEET_NAME")

def main():
    # 🔥 刪除 data資料夾，清除上一次的所有資料
    if os.path.exists('data'):
        shutil.rmtree('data')
    os.makedirs('data')

    print('🔄 執行爬蟲...')
    result = auto_run_spiders()
    if asyncio.iscoroutine(result):
        result = asyncio.run(result)

    print('🌐 傳送資料給 FastAPI /upload...')
    try:
        with open('data/all_cleaned.json', 'r', encoding='utf-8') as f:
            payload = json.load(f)

        res = requests.post(UPLOAD_URL, json=payload)
        res.raise_for_status()

        if res.status_code != 200:
            print(f'❌ FastAPI 回傳錯誤：{res.status_code} → {res.text}')
        else:
            try:
                result = res.json()
            except Exception:
                print('⚠️ FastAPI 回傳非 JSON，原始內容：', res.text)
                result = {'status': 'error', 'message': res.text.strip()}

            print(f'✅ 傳送成功：{res.status_code} / 共 {len(payload)} 筆 → {result}')

    except Exception as e:
        print(f'❌ 傳送失敗：{e}')


if __name__ == '__main__':
    main()
