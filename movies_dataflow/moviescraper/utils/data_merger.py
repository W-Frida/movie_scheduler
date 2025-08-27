import json
from pathlib import Path

def merge_cleaned_outputs(folder="data", pattern="*_formated.json", output="all_cleaned.json"):
    merged = []

    for file in Path(folder).glob(pattern):
        if file.name == output:
            continue

        try:
            with open(file, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, list):
                    for item in data:
                        merged.append(item)

        except Exception as e:
            print(f"[ERROR] 讀取 {file.name} 失敗：{e}")

    output_path = Path(folder)/output
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(merged, f, indent=4, ensure_ascii=False)

    print(f"[MERGED] 成功合併 {len(merged)} 筆資料到 {output_path}")