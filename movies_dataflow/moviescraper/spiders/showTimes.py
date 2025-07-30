import scrapy
from scrapy_selenium4 import SeleniumRequest
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from scrapy.http import HtmlResponse
from collections import defaultdict
from moviescraper.items import MovieItem

class ShowTimeSpider(scrapy.Spider):
    name = 'showtimes'
    custom_settings = {
        'DOWNLOADER_MIDDLEWARES': {
            'scrapy_selenium4.SeleniumMiddleware': 800,
        },
        'SELENIUM_DRIVER_NAME': 'chrome',
        'SELENIUM_DRIVER_EXECUTABLE_PATH': ChromeDriverManager().install(),
        'SELENIUM_DRIVER_ARGUMENTS': ["--headless", "--disable-gpu", "--no-sandbox"]
    }
    allowed_domains = ['showtimes.com.tw']
    start_urls = ['https://www.showtimes.com.tw/ticketing']

    async def start(self):
        yield SeleniumRequest(
            url = self.start_urls[0],
            wait_time = 10,
            callback = self.parse,
        )

    def parse(self, response):
        driver = response.meta['driver']
        try:
            # 等待「影城熱映」分頁本身出現並點擊
            hot_tab = WebDriverWait(driver, 15).until(
                EC.element_to_be_clickable((By.XPATH, '//div[contains(text(), "影城熱映")]'))
            )
            hot_tab.click()
        except Exception as e:
            print(f"⚠️ 秀泰影城等待或點擊失敗：{e}")
            return

        # 等待「影城選單」出現並點擊
        WebDriverWait(driver, 15).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, 'button.sc-iMTnTL'))
        )
        theater_names = [
            btn.text.strip()
            for btn in driver.find_elements(By.CSS_SELECTOR, "button.sc-iMTnTL")
        ]

        for theater_name in theater_names:
            try:
                # 重新抓取對應的按鈕元素再點擊，避免 click() 造成 DOM 更新後元素失效
                theater_btn = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable(
                        (By.XPATH, f'//button[contains(text(), "{theater_name}")]')
                    )
                )
                driver.execute_script("arguments[0].click();", theater_btn)
                WebDriverWait(driver, 15).until(
                    EC.visibility_of_element_located((By.XPATH, '//span[contains(text(), "月")]'))
                )
                date_blocks = driver.find_elements(By.CSS_SELECTOR, 'div.sc-krNlru')[:6]

                for d in range(min(6, len(date_blocks))):
                    date_blocks = driver.find_elements(By.CSS_SELECTOR, 'div.sc-krNlru')
                    date_block = date_blocks[d]

                    driver.execute_script('arguments[0].click()', date_block)

                    # 擷取電影名稱與場次
                    WebDriverWait(driver, 15).until(
                        EC.presence_of_element_located((By.CLASS_NAME, "sc-EgOXT"))
                    )

                    html = driver.page_source
                    scrapy_response = HtmlResponse(url=driver.current_url, body=html, encoding='utf-8')
                    date_texts = scrapy_response.css('div.izhUUl span::text').getall()
                    date_formatted = (f'{date_texts[0]}({date_texts[1]})')
                    movie_blocks = scrapy_response.xpath('//div[div[@class="sc-EgOXT iZnlsW"]]')

                    for movie in movie_blocks:
                        movie_name = movie.css('div.dZNNPl::text').get(default='').strip()
                        has_time_blocks = movie.xpath('.//div[contains(text(), "廳")]')

                        if has_time_blocks:
                            showtime_groups = extract_showtime_info(movie)

                            for group in showtime_groups:
                                item = MovieItem()
                                item['影城'] = theater_name
                                item['網址'] = f'{response.url}'
                                item['電影名稱'] = movie_name
                                item['放映版本'] = group['放映版本']
                                item['日期'] = date_formatted
                                item['時刻表'] = group['時刻表']

                                yield item

            except Exception as e:
                print(f"⚠️ 無法點擊影城 {theater_name} 失敗：{e}")

def group_showtimes_by_version_data(version_showtime_pairs):
    grouped =  defaultdict(list)

    for version, showtime in version_showtime_pairs:
        grouped[version].append(showtime.strip())

    result = []

    for version, times in grouped.items():
        result.append({
            '放映版本': version,
            '時刻表': times
        })

    return result


def extract_showtime_info(movie):
    showtime_blocks = movie.xpath('.//div[div[contains(text(), "廳")]]')
    version_showtime_pairs = []

    for showtime_block in showtime_blocks:
        version_text = showtime_block.xpath('.//div[contains(text(), "廳")]/text()').get(default='版本未知').split('|')[1].strip()
        showtime_text = showtime_block.xpath('.//div[contains(text(), "廳")]/following-sibling::div[1]/text()').get(default='時間未知').split('~')[0].strip()
        version_showtime_pairs.append((version_text, showtime_text))

    return group_showtimes_by_version_data(version_showtime_pairs)
