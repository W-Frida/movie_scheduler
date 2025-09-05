import scrapy
from datetime import datetime
from urllib.parse import urljoin
from moviescraper.items import MovieItem

class vsSpider(scrapy.Spider):
    name = 'vs'
    allowed_domains = ['vscinemas.com.tw']
    start_urls = ['https://www.vscinemas.com.tw/film']

    def parse(self, response):
        movies = response.css('section.infoArea')

        for movie in movies:
            # 過濾尚未上映的電影
            start_date = movie.css('time::text').get()
            start_dt = datetime.strptime(start_date, "%Y-%m-%d")
            if start_dt.date() > datetime.now().date():
                continue

            relative_url = movie.css('h2 a::attr(href)').get()

            if relative_url:
                yield response.follow(relative_url, self.movie_info_parse)

            else:
                self.logger.warning('未找到 vsCinemas_relative_url，請檢查選擇器')

        # 頁數
        next_page = response.css('.pagebar ul li.press + li a::attr(href)').get()
        if next_page is not None:
            next_page_url = urljoin(response.url, next_page)
            yield response.follow(next_page_url, callback=self.parse)

    #威秀影城電影資訊&時刻表
    def movie_info_parse(self, response):

        versionList = response.xpath('//ul[@class="versionList"]/li') #所有放映版本

        for version in versionList:
            theaters = version.css('ul li')

            for theater in theaters:
                theater_name = theater.css('a::text').get()
                movieTimes_id = theater.css('a::attr(href)').get()

                if movieTimes_id:
                    date_blocks = response.css(f'article{movieTimes_id} .movieDay')
                else:
                    date_blocks = []

                # for date_block in date_blocks[:3]:
                if not date_blocks:
                    return
                date_block = date_blocks[0]
                date_text = date_block.css('h4::text').get()
                showtimes = date_block.css('.bookList li a::text').getall()

                # 使用 Item 儲存資料
                item = MovieItem()
                item['影院'] = theater_name
                item['網址'] = self.start_urls[0]
                item['電影名稱'] = response.css('.titleArea h1::text').get()
                item['放映版本'] = version.css('.versionFirst::text').get()
                item['日期'] = date_text.strip().replace(' ', '')
                item['時刻表'] = showtimes

                yield item