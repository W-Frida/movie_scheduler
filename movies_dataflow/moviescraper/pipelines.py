# 多個 pipeline 分層架構，並在 settings.py 設定處理優先順序。
from datetime import datetime
import os, re, json, logging, unicodedata
from .utils.cinema_info import cinema_address_map
from rapidfuzz import fuzz

logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)

class MoviescraperPipeline:
    def __init__(self):
        self.address_map = cinema_address_map
        self.spider_cinema_map = {
            'venice': '威尼斯影城',
            'vs': '威秀影城',
            'sk': '新光影城',
            'amba': '國賓影城',
            'showtimes': '秀泰影城',
            'sbc': '星橋國際影城'
        }
        self.title_pool = []

    def process_item(self, item, spider):
        address = self.match_city_address(item['影院'])
        item['地址'] = address
        item['city'] = address[:2]
        item['cinema'] = self.spider_cinema_map.get(spider.name, '未知影城')
        item['電影名稱'] = self.normalize_title(item.get('電影名稱', ''))
        item['日期'] = self.format_date(item.get('日期', ''), spider.name)
        item["時刻表"] = [t.strip() for t in item["時刻表"]]

        # 國賓影城_放映版本格式
        if spider.name == 'amba':
            version = item.get('放映版本', '')
            match = re.search(r'[（(](.+?)[）)]', version)
            item['放映版本'] = match.group(1) or match.group(2) if match else version

        return item

    def match_city_address(self, cinema_name):
        for key in self.address_map:
            if cinema_name.startswith(key) or key in cinema_name:
                return self.address_map[key]

        return '未知地址'

    def normalize_title(self, title):
        # 將全形轉半形（含標點）
        title = unicodedata.normalize('NFKC', title)   # 全形轉半形
        title = re.sub(r'[「」『』“”‘’:：_．・.]', ' ', title)     # 移除中英文符號
        title = re.sub(r'\s+', ' ', title).strip()     # 合併空格並去除首尾空白

        # 模糊比對
        for known in self.title_pool:
            scores = {
                "token_set": fuzz.token_set_ratio(title, known),
                "token_sort": fuzz.token_sort_ratio(title, known),
                "partial": fuzz.partial_ratio(title, known),
                "ratio": fuzz.ratio(title, known),
                "wratio": fuzz.WRatio(title, known)
            }
            best_score = max(scores.values())
            if best_score >= 90:
                return known
            else:
                logging.debug(f"🧪 tried: '{title}' vs '{known}' → score={best_score}")

        self.title_pool.append(title)
        logging.warning(f"⚠️ 未比對成功，新增標題：'{title}'")
        return title

    def format_date(self, raw_date, spider_name='unknown'):
        date_str = raw_date.strip()
        today = datetime.today().date()

        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", date_str):
            return date_str

        patterns = [
            {"pattern": r"(\d{4})-(\d{1,2})-(\d{1,2})\(星期.\)", "year":1, "month":2, "day":3},
            {"pattern": r"(\d{4})年(\d{1,2})月(\d{1,2})日(星期.)", "year":1, "month":2, "day":3},
            {"pattern": r"(\d{1,2})-(\d{1,2})\(.\)", "year": None, "month":1, "day":2},
            {"pattern": r"(\d{1,2})月(\d{1,2})日\(周.\)", "year": None, "month":1, "day":2},
            {"pattern": r".*?(\d{4})/(\d{1,2})/(\d{1,2})", "year":1, "month":2, "day":3}
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
                return f'{dt.strftime("%Y-%m-%d")}'

            except Exception as e:
                logger.warning(f"[{spider_name}] 日期解析失敗: '{date_str}' → {e}")
                continue

        logger.warning(f"[{spider_name}] 無法解析日期格式: '{date_str}'")
        return raw_date  # 如果無法解析，就原樣返回

# 存檔: data/*_formated.json
class JsonExportPipeline:
    def open_spider(self, spider):
        self.items = []

    def close_spider(self, spider):
        os.makedirs("data", exist_ok=True)
        with open(f"data/{spider.name}_formated.json", "w", encoding="utf-8") as f:
            json.dump(self.items, f, indent=4, ensure_ascii=False)
            print(f"{spider.name}_formated.json saved")

    def process_item(self, item, spider):
        self.items.append(dict(item))
        return item



