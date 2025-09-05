import scrapy
from moviescraper.items import MovieItem

class AmbassadorSpider(scrapy.Spider):
    name = 'amba'
    allowed_domains = ['ambassador.com.tw']
    start_urls = ['https://www.ambassador.com.tw/home/MovieList?Type=1']

    def parse(self, response):
        movies = response.css('#tab1 .cell')

        for movie in movies:
            if movie.css('.title span.close::text').get() is None:
                continue

            relative_url = movie.css('a.poster::attr(href)').get()

            if relative_url:
                yield response.follow(relative_url, self.movieInfo_parse)
            else:
                self.logger.warning('未找到國賓影城的 relative_url，請檢查選擇器')

    #從日期搜尋
    def movieInfo_parse(self, response):
        date_urls = response.css('#search-bar-page ul.scrollbar li')

        for date_url in date_urls[:3]:
            relative_url = date_url.css('a::attr(href)').get()
            date_text = date_url.css('a::text').get() or '未知日期'

            yield response.follow(
                relative_url,
                self.movieTimes_parse,
                meta={'date' : date_text},
                dont_filter=True # ✅ 強制執行，即使 URL 重複
            )

    #各影院時刻表
    def movieTimes_parse(self, response):
        movie_name = response.css('div.movie-info-box h2::text').get()
        theaters = response.css('.theater-box')

        for theater in theaters:
            # 使用 Item 儲存資料
            item = MovieItem()
            item['影院'] = theater.css('h3 a::text').get()
            item['網址'] = self.start_urls[0]
            item['電影名稱'] = movie_name
            item['放映版本'] = theater.css('p.tag-seat::text').get()
            item['日期'] = response.meta['date']
            item['時刻表'] = theater.css('ul.no-bullet li h6::text').getall()

            yield item