import scrapy, time
from moviescraper.items import MovieItem
from datetime import datetime
from scrapy_selenium4 import SeleniumRequest
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from scrapy.http import HtmlResponse

class sbcSpider(scrapy.Spider):
    name = "sbc"
    allowed_domains = ["sbcmovies.com.tw"]
    url = "https://www.sbcmovies.com.tw/browsing/Movies/NowShowing"
    custom_settings = {
        "DOWNLOADER_MIDDLEWARES": {
            "scrapy_selenium4.SeleniumMiddleware": 800,
        },
        "SELENIUM_DRIVER_NAME": "chrome",
        "SELENIUM_DRIVER_EXECUTABLE_PATH": ChromeDriverManager().install(),
        "SELENIUM_DRIVER_ARGUMENTS": ["--headless", "--disable-gpu", "--no-sandbox"]
    }

    def start_requests(self):
        self.logger.info("🚀 發送 SeleniumRequest 至星橋票務頁面")
        yield SeleniumRequest(
            url = self.url,
            wait_time = 10,
            callback = self.parse,
        )

    def parse(self, response):
        driver = response.meta.get("driver")
        if not driver:
            self.logger.error("❌ Selenium driver not found in response.meta")
            return

        try:
            # 等待「語言按鈕」出現並點擊切換語言
            lang_btn = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.ID, "change-language"))
            )
            self.logger.info("語言切換按鈕已找到，準備點擊")
            lang_btn.click()
        except Exception as e:
            self.logger.warning(f"⚠️ 星橋影城語言切換失敗：{e}")
            return

        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "#movies-list"))
        )

        # ✅ 擷取語言切換後的 cookie
        cookies = {c['name']: c['value'] for c in driver.get_cookies()}
        self.logger.debug(f"🍪 擷取 cookie：{cookies}")

        body = driver.page_source
        response = HtmlResponse(url=driver.current_url, body=body, encoding="utf-8")
        movies = response.css("#movies-list .list-item")

        for movie in movies:
            relative_url = movie.css("div.main-action a::attr(href)").get()
            if relative_url:
                    yield response.follow(relative_url, self.movieInfo_parse, cookies=cookies)
            else:
                self.logger.warning('未找到星橋影城的 relative_url，請檢查選擇器')

    def movieInfo_parse(self, response):
        movie_info = response.css("#show-times")

        # 過濾未上映的電影
        latest_dateTimes = movie_info.css("div.session .session-times time::attr(datetime)").get()
        latest_dt = datetime.strptime(latest_dateTimes, "%Y-%m-%dT%H:%M:%S")
        if latest_dt.date() > datetime.now().date():
            return

        raw_title = movie_info.css("h2::text").get(default="").strip()
        title_split = self.parse_title_by_suffix(raw_title)

        date_blocks = movie_info.css("div.film-showtimes .session")
        if not date_blocks:
            return
        
        # for date_block in date_blocks[0]:
        date_block = date_blocks[0]
        raw_datetimes = date_block.css("div.session-times time::attr(datetime)").getall()
        date_str = ""
        time_list = []

        for raw_dt in raw_datetimes:
            dt_obj = datetime.strptime(raw_dt, "%Y-%m-%dT%H:%M:%S")
            date_str = dt_obj.strftime("%Y-%m-%d")
            time_list.append(dt_obj.strftime("%H:%M"))

        item = MovieItem()
        item['影院'] = '星橋國際影城'
        item['網址'] = self.url
        item['電影名稱'] = title_split["電影名稱"]
        item['放映版本'] = title_split["放映版本"]
        item['日期'] = date_str if date_str else "未知日期"
        item['時刻表'] = time_list

        yield item

    @staticmethod
    def parse_title_by_suffix(raw_title):
        suffix_keywords = ["語", "中文", "日文", "英文", "發音"]
        parts = raw_title.strip().split()

        if len(parts) == 1:
            return {"電影名稱": parts[0], "放映版本": "未知版本"}

        first, *rest = parts
        if any(first.endswith(suffix) for suffix in suffix_keywords):
            return {
                "電影名稱": " ".join(rest) if rest else "未知電影",
                "放映版本": first
            }
        elif any(rest[-1].endswith(suffix) for suffix in suffix_keywords):
            return {
                "電影名稱": " ".join(parts[:-1]),
                "放映版本": rest[-1]
            }
        else:
            return {
                "電影名稱": " ".join(parts),
                "放映版本": "未知版本"
            }