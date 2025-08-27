import os, json, requests, shutil, asyncio, sys, argparse
from spider_executor import SpiderExecutor
from dotenv import load_dotenv
from moviescraper.utils.data_merger import merge_cleaned_outputs

# ✅ Windows asyncio reactor 相容性處理
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# ✅ 環境變數載入
load_dotenv()
BASE_URL = os.getenv("BASE_URL", "http://localhost:8000")
UPLOAD_URL = f"{BASE_URL}/upload"
CREDENTIALS_PATH = os.getenv("CREDENTIALS_PATH", "/etc/secrets/credentials.json")
SPREADSHEET_NAME = os.getenv("SPREADSHEET_NAME")

# ✅ 清除並建立 data 資料夾
def clean_data_folder():
    if os.path.exists("data"):
        shutil.rmtree("data")
    os.makedirs("data")

# ✅ 上傳資料至 FastAPI
def upload_to_fastapi(json_path="data/all_cleaned.json"):
    try:
        with open(json_path, encoding="utf-8") as f:
            payload = json.load(f)
        res = requests.post(UPLOAD_URL, json=payload)
        res.raise_for_status()
        try:
            result = res.json()
        except Exception:
            print("⚠️ FastAPI 回傳非 JSON，原始內容：", res.text)
            result = {"status": "error", "message": res.text.strip()}
        print(f'✅ 傳送成功：{res.status_code} / 共 {len(payload)} 筆 → {result}')
    except Exception as e:
        print(f'❌ 傳送失敗：{e}')


# ✅ 主執行流程
def main():
    parser = argparse.ArgumentParser(description="執行 Scrapy 並上傳結果")
    parser.add_argument("--mode", default="subprocess", choices=["cli", "async", "subprocess"], help="執行模式")
    parser.add_argument("--targets", type=str, help="指定爬蟲名稱（用逗號分隔）")
    parser.add_argument("--no-upload", action="store_true", help="跳過上傳步驟")
    parser.add_argument("--dry-run", action="store_true", help="僅執行爬蟲，不合併、不上傳")
    args = parser.parse_args()

    spiders = args.targets.split(",") if args.targets else None

    print(f"目前進度: 清除 data 資料夾")
    clean_data_folder()

    print(f"目前進度: 啟動 Scrapy → 模式: {args.mode} / {spiders or '全部'}")
    SpiderExecutor().run(mode=args.mode, spiders=spiders)

    if args.dry_run:
        print("🧪 Dry-run 模式 → 跳過合併與上傳")
        return

    print(f"目前進度: 合併所有影城資料 → 匯出 all_cleaned.json")
    merge_cleaned_outputs("data", "*_formated.json", "all_cleaned.json")

    if args.no_upload:
        print("📦 No-upload 模式 → 已完成合併，但不執行上傳")
        return

    print('目前進度: 傳送資料給 FastAPI /upload...')
    upload_to_fastapi()


if __name__ == '__main__':
    main()
