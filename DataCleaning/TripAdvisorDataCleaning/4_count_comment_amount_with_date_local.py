from datetime import datetime, timedelta
from pymongo import MongoClient
import logging
import pandas as pd

# 計算mongoDB collection 中的個別餐廳評論數 及 符合日期條件的評論數
def extract_comments_from_mongodb(db_name: str, collection_name: str):
    try:
        # 連接到 MongoDB
        client = MongoClient('mongodb://localhost:27017/')  # 替換為您的 MongoDB 連接字串
        db = client[db_name]  # 選擇資料庫
        collection = db[collection_name]  # 選擇集合（表）

        # 初始化一個空的列表來儲存數據
        data = []

        # 提取第 1 到第 5 筆資料
        for restaurant in collection.find().skip(0).limit(1200):  # skip(0) 表示從第一筆開始
            restaurant_id = restaurant.get('_id')  # 提取餐廳ID
            comments = restaurant.get('comments_Info')  # 提取評論資料

            # 初始化計數器
            total_comments = 0
            matching_comments = 0
            matching_char_count = 0  # 符合條件的評論字元數

            # 檢查 comments 是否為列表
            if isinstance(comments, list):
                total_comments = len(comments)  # 總評論數

                # 逐條分析評論內容
                for item in comments:
                    comment_date = item.get('comment_date')
                    comment_text = item.get('comment_content', '')  # 提取評論文字，預設為空字串

                    # 確保 comment_date 是 datetime 對象
                    if isinstance(comment_date, str):
                        comment_date = datetime.fromisoformat(comment_date.replace("Z", "+00:00"))

                    one_year_before = datetime.now() - timedelta(days=370)  # 一年前的日期
                    # 檢查 comment_date 是否在 2024-02-01 之後
                    if comment_date and comment_date > one_year_before:
                        matching_comments += 1  # 累加符合條件的評論數
                        matching_char_count += len(comment_text)  # 累加符合條件的評論字元數

            # 打印每間餐廳的評論數和符合條件的評論數及字元數
            print(f"餐廳ID: {restaurant_id}, 總評論數: {total_comments}, 2024-02-01後的評論數: {matching_comments}, 符合條件的評論字元數: {matching_char_count}")

            # 將數據添加到列表中
            data.append([restaurant_id, total_comments, matching_comments, matching_char_count])

        # 將數據轉換為 DataFrame
        df = pd.DataFrame(data, columns=['餐廳ID', '總評論數', '2024-02-01後的評論數', '符合條件的評論字元數'])

        # 將 DataFrame 存儲為 CSV 檔案
        df.to_csv("comments_count_0304.csv", index=False, encoding='utf-8')

    except Exception as e:
        logging.error(f"提取評論時出錯: {e}")
        print("出錯，無法提取評論數")

    finally:
        # 確保關閉連接
        client.close()

extract_comments_from_mongodb('tripadvisor', 'google_comment_v6')