import scrapy
from scrapy_selenium4 import SeleniumRequest
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from moviescraper.items import MovieItem

class skSpider(scrapy.Spider):
    name = 'sk'
    custom_settings = {
        'DOWNLOADER_MIDDLEWARES': {
            'scrapy_selenium4.SeleniumMiddleware': 800,
        },
        'SELENIUM_DRIVER_NAME': 'chrome',
        'SELENIUM_DRIVER_EXECUTABLE_PATH': ChromeDriverManager().install(),
        'SELENIUM_DRIVER_ARGUMENTS': ["--headless", "--disable-gpu", "--no-sandbox"]
    }
    allowed_domains = ['skcinemas.com']
    start_urls = ['https://www.skcinemas.com/sessions?c=1001']

    async def start(self):
        yield SeleniumRequest(
            url = self.start_urls[0],
            wait_time = 10,
            callback = self.parse,
        )

    def parse(self, response):
        driver = response.meta['driver']
        WebDriverWait(driver, 15).until(EC.visibility_of_element_located((By.CSS_SELECTOR, 'div.route-items')))

        # 點擊影城
        for i in range(5):
            cinema_buttons = driver.find_elements(By.CSS_SELECTOR, '.route-item')  # 重新獲取元素

            if i >= len(cinema_buttons):  # 避免超出範圍
                print(f'錯誤:沒有第 {i} 個新光影城。')
                break

            cinema_buttons[i].click()
            try:
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, 'div.movie-sessions-view'))
                )
            except TimeoutException:
                print(f'⚠️ 新光影城 {i} 載入時間過長，跳過')
                continue

            new_html = driver.page_source
            response = scrapy.Selector(text=new_html)
            yield from self.movie_data(response)


    def movie_data(self, response):
        cinema_name = response.css('div.route-items .active .title::text').get()
        movies = response.css('div.movie-sessions-view')

        for movie in movies:
            date_blocks = movie.css('.day-sessions')
            for date_block in date_blocks[:3]:
                version = date_block.css('.film-type::text').get() or '版本未知'
                date_text = date_block.css('.business-date::text').get()
                showtimes = date_block.css('.session::text').getall()

                item = MovieItem() # 使用 Item 儲存資料
                item['影院'] = cinema_name
                item['網址'] = self.start_urls[0]
                item['電影名稱'] = movie.css('.film-name::text').get()
                item['放映版本'] = version
                item['日期'] = date_text.strip().replace(' ', '') or '日期未知'
                item['時刻表'] = showtimes

                yield item
