#威尼斯影城
import scrapy
import re
from urllib.parse import urljoin
from moviescraper.items import MovieItem

class VeniceSpider(scrapy.Spider):
    name = 'vn'
    allowed_domains = ['venice-cinemas.com.tw']
    start_urls = ['https://www.venice-cinemas.com.tw/showtime.php']

    def parse(self, response):
        try:
            options = response.css("#search_movie option")
            for opt in options:
                msn = opt.css("::attr(value)").get()
                sn = opt.css("::attr(data-sn)").get()

                if not msn or not sn:
                    continue

                url = f"https://www.venice-cinemas.com.tw/showtime-view.php?sn={sn}&msn={msn}"
                yield scrapy.Request(url, callback=self.showtimes_parse)

        except Exception as e:
            print("找不到 movies 的網址參數")

    # 時刻表
    def showtimes_parse(self, response):
        if response.status == 522:
            self.logger.warning(f"⚠️ 威尼斯影城: 522 錯誤 → {response.url}")
            return

        raw_movie_name = response.css('.show-time-view h2::text').get()
        match = re.search(r'(.*?)\((.*?)\)', raw_movie_name)
        movie_name = match.group(1).strip() if match else raw_movie_name.strip()
        version = match.group(2).strip() if match else '未知版本'

        date_blocks = response.css('.show-time')
        for date_block in date_blocks[:3]:
            date_text = date_block.css('.showtime-date::text').get()
            showtimes = date_block.css('.showtime-item label::text').getall()

            # 使用 Item 儲存資料
            item = MovieItem()
            item['影院'] = '威尼斯影城'
            item['網址'] = self.start_urls[0]
            item['電影名稱'] = movie_name
            item['放映版本'] = version
            item['日期'] = date_text.strip().replace(' ', '') if date_text else '未知日期'
            item['時刻表'] = showtimes if showtimes else ['未知時間']

            yield item