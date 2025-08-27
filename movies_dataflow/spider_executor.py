import time, sys, subprocess
from pathlib import Path
from scrapy.crawler import CrawlerRunner, CrawlerProcess
from scrapy.utils.project import get_project_settings
from moviescraper.spiders import amba, showTimes, sk, vs, venice

SPIDER_MAP = {
    'amba': amba.AmbassadorSpider,
    'showtimes': showTimes.ShowTimeSpider,
    'sk': sk.skSpider,
    'vs': vs.vsSpider,
    'venice': venice.VeniceSpider,
}

class SpiderExecutor:
    def __init__(self):
        self.report = {}

    def run(self, mode="cli", spiders=None):
        if mode == "cli":
            self.run_cli(spiders)
        elif mode == "async":
            self.run_async(spiders)
        elif mode == "subprocess":
            self.run_subprocess(spiders)
        else:
            raise ValueError(f"❌ 不支援的執行模式：{mode}")

    def run_cli(self, spiders=None):
        print("🖥️ CLI 模式 → 使用 CrawlerProcess")
        process = CrawlerProcess(get_project_settings())

        selected = spiders or list(SPIDER_MAP.keys())
        for name in selected:
            spider_cls = SPIDER_MAP.get(name)
            if not spider_cls:
                print(f"⚠️ 未知爬蟲名稱：{name}")
                continue
            self.report[name] = {'start': time.time()}
            process.crawl(spider_cls)
            print(f"🕷️ 已註冊爬蟲：{name}")

        try:
            print("🚀 Scrapy process 即將啟動")
            process.start()
            print("✅ Scrapy process 已結束")
        except Exception as e:
            print(f"⚠️ CLI模式 執行失敗：{e}")

        self._finish_report()

    def run_async(self, spiders=None):
        print("🌐 非同步模式 → 使用 CrawlerRunner")
        from scrapy.utils.reactor import install_reactor
        install_reactor("twisted.internet.asyncioreactor.AsyncioSelectorReactor")
        from twisted.internet import reactor, defer

        selected = spiders or list(SPIDER_MAP.keys())

        @defer.inlineCallbacks
        def _run():
            runner = CrawlerRunner(get_project_settings())

            for name in selected:
                spider_cls = SPIDER_MAP.get(name)
                if not spider_cls:
                    print(f"⚠️ 未知爬蟲名稱：{name}")
                    continue
                self.report[name] = {'start': time.time()}
                try:
                    yield runner.crawl(spider_cls)
                    print(f"✅ {name} 執行完成")
                except Exception as e:
                    print(f"⚠️ async模式 {name} 執行失敗: {e}")

            self._finish_report()
            reactor.callLater(30, reactor.stop)  # 最多執行 30 秒 # 排程停止

        reactor.callWhenRunning(_run) # 註冊 callback (註冊所有非同步任務)
        reactor.run() # 啟動事件循環 → 開始執行 _run() 裡的 yield 任務


    def run_subprocess(self, spiders=None):
        print("🌐 使用 subprocess 包裝 CLI")
        spider_path = Path(__file__)
        args = ["--cli"]
        if spiders:
            args.append("--targets=" + ",".join(spiders))
        result = subprocess.run([sys.executable, str(spider_path)] + args)
        if result.returncode != 0:
            print(f"⚠️ subprocess returncode 非 0：{result.returncode}")
        else:
            print("✅ subprocess 執行成功")

    def _finish_report(self):
        print("\n✅ 所有爬蟲完成，執行時間如下：")
        for name, info in self.report.items():
            end = time.time()
            duration = end - info['start']
            minutes, seconds = divmod(duration, 60)
            print(f"- {name}: {int(minutes)} 分 {int(seconds)} 秒")

# ✅ subprocess 呼叫入口
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="執行 Scrapy 爬蟲")
    parser.add_argument("--mode", default="cli", choices=["cli", "async", "subprocess"])
    parser.add_argument("--targets", type=str, help="指定爬蟲名稱（用逗號分隔）")
    args = parser.parse_args()

    spiders = args.targets.split(",") if args.targets else None
    SpiderExecutor().run(mode=args.mode, spiders=spiders)

