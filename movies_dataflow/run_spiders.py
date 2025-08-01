import asyncio, sys

# ✅ Windows 特殊處理選擇 AsyncioSelectorReactor → 與 Scrapy 相容
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# ✅ 在任何 Twisted import 之前安裝 reactor
import scrapy.utils.reactor
scrapy.utils.reactor.install_reactor("twisted.internet.asyncioreactor.AsyncioSelectorReactor")


import subprocess, logging, time
from pathlib import Path
from scrapy.crawler import CrawlerProcess, CrawlerRunner
from scrapy.utils.project import get_project_settings
from twisted.internet import reactor, defer
from twisted.internet.error import ReactorNotRestartable

from moviescraper.spiders import amba, showTimes, sk, vs, venice
from moviescraper.utils.data_merger import merge_cleaned_outputs

SPIDER_MAP = {
    'amba': amba.AmbassadorSpider,
    'showtimes': showTimes.ShowTimeSpider,
    'sk': sk.skSpider,
    'vs': vs.vsSpider,
    'venice': venice.VeniceSpider,
}

def finish_report(report):
    for name in report:
        end = time.time()
        duration = end - report[name]['start']
        minutes, seconds = divmod(duration, 60)
        report[name]['end'] = end
        report[name]['duration'] = f"{int(minutes)} 分 {int(seconds)} 秒"
    return report

def print_report(report):
    print("\n✅ 所有爬蟲完成，執行時間如下：")
    for name, info in report.items():
        print(f"- {name}: {info['duration']}")


# ------------------------------------------------------------------------------------------
# 🖥️ CLI 模式：同步執行，阻塞主執行緒
# ------------------------------------------------------------------------------------------
def run_all_spiders():
    logging.getLogger('scrapy').setLevel(logging.DEBUG)
    settings = get_project_settings()
    process = CrawlerProcess(settings)

    report = {}
    for name, spider_cls in SPIDER_MAP.items():
        report[name] = {'start': time.time()}
        process.crawl(spider_cls)

    try:
        process.start()
    except ReactorNotRestartable as e:
        print(f"⚠️ Reactor 無法重啟：{e}")

    report = finish_report(report)
    print_report(report)

    # 🧩 執行合併模組，把所有 *_formated.json 整合成 all_cleaned.json
    merge_cleaned_outputs(folder="data", pattern="*_formated.json", output="all_cleaned.json")
    print("[MERGE] ✅ 合併完成 → 輸出 all_cleaned.json")

# -------------------------------------------------------------------------------------------
# 🌐 FastAPI webhook / async 模式：非阻塞 coroutine
# -------------------------------------------------------------------------------------------
@defer.inlineCallbacks
def _crawl_deferred():
    runner = CrawlerRunner(get_project_settings())
    for spider_cls in SPIDER_MAP.values():
        yield runner.crawl(spider_cls)
    reactor.stop()

async def run_safe_spiders():
    print("🌐 已切換為 subprocess 模式，觸發 CLI runner（避免 Twisted reactor.signal 錯誤）")
    try:
        spider_path = Path(__file__)
        result = await asyncio.to_thread(
            subprocess.run,
            ["python", str(spider_path), "--cli", "--source=webhook"]
        )
    except Exception as e:
        print(f"❌ subprocess 執行失敗，例外: {e}")
        return

    if result.returncode != 0:
        print(f"⚠️ subprocess 執行結束，但 returncode 非 0：{result.returncode}")
    else:
        print("✅ subprocess 執行成功，爬蟲已完成")

    # 🧩 執行合併模組，把所有 *_formated.json 整合成 all_cleaned.json
    merge_cleaned_outputs(folder="data", pattern="*_formated.json", output="all_cleaned.json")
    print("[MERGE] ✅ 合併完成 → 輸出 all_cleaned.json")

# -------------------------------------------------------------------------------------------
# 🧠 自動偵測入口點
# -------------------------------------------------------------------------------------------
def auto_run_spiders():
    if "--cli" in sys.argv:
        # 直接 CLI 執行
        print("🖥️ 偵測到 CLI 模式 → 執行 run_all_spiders()")
        run_all_spiders()
    else:
        # 被 FastAPI webhook 或 task queue 呼叫
        print("🌐 偵測到非同步模式 → 執行 run_safe_spiders()")
        return run_safe_spiders()


if __name__ == "__main__":
    auto_run_spiders()
