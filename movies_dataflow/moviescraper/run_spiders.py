from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from moviescraper.spiders.amba import AmbassadorSpider
from moviescraper.spiders.showTimes import ShowTimeSpider
from moviescraper.spiders.sk import skSpider
from moviescraper.spiders.vs import vsSpider
from moviescraper.spiders.venice import VeniceSpider

from moviescraper.utils.data_merger import merge_cleaned_outputs

import logging
import time
import json

SPIDER_MAP = {
    'amba': AmbassadorSpider,
    'showtimes': ShowTimeSpider,
    'sk': skSpider,
    'vs': vsSpider,
    'venice': VeniceSpider,
}

def run_all_spiders():
    settings = get_project_settings()
    process = CrawlerProcess(settings)

    report = {}
    for name, spider_cls in SPIDER_MAP.items():
        start = time.time()
        process.crawl(spider_cls)
        report[name] = {'start': start}

    process.start()

    for name in report:
        end = time.time()
        duration = end - report[name]['start']
        minutes, seconds = divmod(duration, 60)
        report[name]['duration'] = f"{int(minutes)} 分 {int(seconds)} 秒"
        report[name]['end'] = end

    print("\n✅ 所有爬蟲完成，執行時間如下：")
    for name, info in report.items():
        print(f"- {name}: {info['duration']}")

    # 🧩 執行合併模組，把所有 *_formated.json 整合成 all_cleaned.json
    print("\n[MERGE] 開始合併所有 cleaned JSON...")
    merge_cleaned_outputs(folder="data", pattern="*_formated.json", output="all_cleaned.json")
    print("[MERGE] ✅ 合併完成 → 輸出 all_cleaned.json")

if __name__ == "__main__":
    logging.getLogger('scrapy').setLevel(logging.DEBUG)  # 如需更詳細可改成 INFO
    run_all_spiders()