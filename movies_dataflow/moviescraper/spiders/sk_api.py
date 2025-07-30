import scrapy
import json
from moviescraper.items import MovieItem

class SkApiSpider(scrapy.Spider):
    name = 'skapi'
    allowed_domains = ['https://www.skcinemas.com']
    custom_settings = {
        'DOWNLOAD_DELAY': 1.0,  # 加點延遲避免頻繁請求
    }
    start_urls = ['https://www.skcinemas.com/api/VistaDataV2/GetHomePageListForApps']

    homepage_api = "https://www.skcinemas.com/api/VistaDataV2/GetHomePageListForApps"
    cinemas_api = "https://www.skcinemas.com/api/VistaDataV2/GetAllForApp"
    session_api = "https://www.skcinemas.com/api/VistaDataV2/GetSessionByCinemasIDForApp"

    def start_requests(self):
        yield scrapy.Request(
            url=self.homepage_api,
            method="POST",
            headers={"Content-Type": "application/json"},
            body="{}",
            callback=self.parse_movies
        )

    def parse_movies(self, response):
        data = response.json()
        self.movie_map = {}
        for movie in data.get("Result", {}).get("NowShowing", []):
            movie_id = movie.get("FilmRowId")
            self.movie_map[movie_id] = {
                "title": movie.get("FilmName"),
                "english_name": movie.get("FilmEnName"),
                "poster": movie.get("FilmPoster"),
                "rating": movie.get("Rating"),
                "runtime": movie.get("RunTime"),
                "type": movie.get("Genre")
            }

        # 接著呼叫影城資料 API
        yield scrapy.Request(
            url=self.cinemas_api,
            method="POST",
            headers={"Content-Type": "application/json"},
            body="{}",
            callback=self.parse_cinemas
        )

    def parse_cinemas(self, response):
        data = response.json()
        cinema_ids = []
        self.cinema_map = {}

        for cinema in data.get("Cinemas", []):
            cid = cinema.get("CinemaCode")
            cinema_ids.append(cid)
            self.cinema_map[cid] = cinema.get("CinemaName")

        # 呼叫所有影城場次 API
        body = {"cinemas": cinema_ids}
        yield scrapy.Request(
            url=self.session_api,
            method="POST",
            headers={"Content-Type": "application/json"},
            body=json.dumps(body),
            callback=self.parse_sessions
        )

    def parse_sessions(self, response):
        data = response.json()

        for session in data.get("Sessions", []):
            movie_id = session.get("FilmRowId")
            cinema_id = session.get("CinemaRowId")
            movie_info = self.movie_map.get(movie_id, {})
            cinema_name = self.cinema_map.get(cinema_id, "未知影城")

            item = MovieItem()
            item['影城'] = cinema_name
            item['網址'] = response.url
            item['電影名稱'] = movie_info.get("title", "未知電影")
            item['英文名稱'] = movie_info.get("english_name")
            # item['類型'] = movie_info.get("type")
            # item['片長'] = movie_info.get("runtime")
            # item['分級'] = movie_info.get("rating")
            # item['電影海報'] = movie_info.get("poster")
            item['放映版本'] = session.get("FilmFormats")
            item['日期'] = session.get("BusinessDate")
            item['時間'] = session.get("SessionTime")

            yield item
