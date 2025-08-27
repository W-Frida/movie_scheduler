# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class MoviescraperItem(scrapy.Item):
    # define the fields for your item here like:
    name = scrapy.Field()
    pass

class MovieItem(scrapy.Item):
    city = scrapy.Field()
    cinema = scrapy.Field()
    影城 = scrapy.Field()
    地址 = scrapy.Field()
    網址 = scrapy.Field()
    電影名稱 = scrapy.Field()
    放映版本 = scrapy.Field()
    # 片長 = scrapy.Field()
    # 類型 = scrapy.Field()
    # 級數 = scrapy.Field()
    # 電影介紹 = scrapy.Field()
    日期 = scrapy.Field()
    時刻表 = scrapy.Field()