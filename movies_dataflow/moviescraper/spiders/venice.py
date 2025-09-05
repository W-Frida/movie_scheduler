#威尼斯影城
import scrapy
import re
from urllib.parse import urljoin
from moviescraper.items import MovieItem

class VeniceSpider(scrapy.Spider):
    name = 'venice'
    allowed_domains = ['venice-cinemas.com.tw']
    start_urls = ['https://venice-cinemas.com.tw/movie.php']

    def parse(self, response):
        for movie in response.css('div.movie-list'):
            relative_url = movie.css('.read-more a::attr(href)').get()

            if relative_url:
                yield response.follow(relative_url, self.movie_info_parse)
            else:
                self.logger.warning('未找到威尼斯影城的 relative_url，請檢查選擇器')

        # 頁數
        next_page = response.css('.pagination li.active + li a::attr(href)').get()
        if next_page is not None:
            next_page_url = urljoin(response.url, next_page)
            yield response.follow(next_page_url, callback=self.parse)

    # 各放映版本的電影資訊
    def movie_info_parse(self, response):
        versionList = response.css('.showtimeBox li a::attr(href)').getall()

        for url in versionList:
            yield response.follow(url, self.showtimes_parse)

    # 時刻表
    def showtimes_parse(self, response):
        raw_movie_name = response.css('.show-time-view h2::text').get()
        match = re.search(r'(.*?)\((.*?)\)', raw_movie_name)
        movie_name = match.group(1).strip() if match else raw_movie_name.strip()
        version = match.group(2).strip() if match else '未知版本'

        date_blocks = response.css('.show-time')
        if not date_blocks:
            return
        # for date_block in date_blocks[:3]:
        date_block = date_blocks[0]
        date_text = date_block.css('.showtime-date::text').get()
        showtimes = date_block.css('.showtime-item label::text').getall()

        # 使用 Item 儲存資料
        item = MovieItem()
        item['影院'] = response.css('title::text').get(default='未知影城').strip()
        item['網址'] = self.start_urls[0]
        item['電影名稱'] = movie_name
        item['放映版本'] = version
        item['日期'] = date_text.strip().replace(' ', '') if date_text else '未知日期'
        item['時刻表'] = showtimes if showtimes else ['未知時間']

        yield item