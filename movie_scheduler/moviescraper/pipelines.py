# 多個 pipeline 分層架構，並在 settings.py 設定處理優先順序。
from datetime import datetime
import os
from itemadapter import ItemAdapter
from .utils.cinema_info import cinema_address_map
import re
import json

class MoviescraperPipeline:
    def __init__(self):
        self.address_map = cinema_address_map
        self.weekday_map = ['一', '二', '三', '四', '五', '六', '日']
        self.spider_cinema_map = {
            'venice': '威尼斯影城',
            'vs': '威秀影城',
            'sk': '新光影城',
            'amba': '國賓影城',
            'showtimes': '秀泰影城'
        }

    def process_item(self, item, spider):
        item['city'] = self.match_city_address(item['影城'])
        item['cinema'] = self.spider_cinema_map.get(spider.name, '未知影城')
        item['日期'] = self.format_date(item.get('日期', ''))
        item["時刻表"] = [t.strip() for t in item["時刻表"]]

        # 國賓影城_放映版本格式
        if spider.name == 'amba':
            version = item.get('放映版本', '')
            match = re.search(r'（(.+?)）|[(](.+?)[)]', version)
            item['放映版本'] = match.group(1) or match.group(2) if match else version

        return item

    def match_city_address(self, cinema_name):
        for key in self.address_map:
            if cinema_name.startswith(key) or key in cinema_name:
                return self.address_map[key][:3]

        return '未知地址'

    def format_date(self, raw_date):
        date_str = raw_date.strip()
        today = datetime.today().date()
        patterns = [
            {"pattern": r"(\d{4})-(\d{1,2})-(\d{1,2})\(星期.\)", "year":1, "month":2, "day":3},
            {"pattern": r"(\d{4})年(\d{1,2})月(\d{1,2})日(星期.)", "year":1, "month":2, "day":3},
            {"pattern": r"(\d{1,2})-(\d{1,2})\(.\)", "year": None, "month":1, "day":2},
            {"pattern": r"(\d{1,2})月(\d{1,2})日\(周.\)", "year": None, "month":1, "day":2},
            {"pattern": r"週.\s*,\s*(\d{4})/(\d{1,2})/(\d{1,2})", "year":1, "month":2, "day":3},
            {"pattern": r"當.\s*,\s*(\d{4})/(\d{1,2})/(\d{1,2})", "year":1, "month":2, "day":3},
        ]

        for p in patterns:
            match = re.search(p["pattern"], date_str)
            if not match:
                continue
            try:
                y = int(match.group(p["year"])) if p["year"] else today.year
                m = int(match.group(p["month"]))
                d = int(match.group(p["day"]))
                dt = datetime(y, m, d).date()
                # 若已過 → 補成下一年
                if dt < today:
                    y += 1
                    dt = datetime(y, m, d).date()
                weekday = self.weekday_map[dt.weekday()]
                return f'{dt.strftime("%Y-%m-%d")}({weekday})'

            except Exception as e:
                continue

        return raw_date  # 如果無法解析，就原樣返回

# 存檔: data/*_formated.json
class JsonExportPipeline:
    def open_spider(self, spider):
        self.items = []

    def close_spider(self, spider):
        os.makedirs("data", exist_ok=True)
        with open(f"data/{spider.name}_formated.json", "w", encoding="utf-8") as f:
            json.dump(self.items, f, indent=4, ensure_ascii=False)

    def process_item(self, item, spider):
        self.items.append(dict(item))
        return item



