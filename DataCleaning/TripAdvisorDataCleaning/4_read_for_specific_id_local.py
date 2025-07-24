import os
import logging
import pandas as pd
from pymongo import MongoClient
from openai import OpenAI
from datetime import datetime

def extract_comments_from_mongodb(db_name: str, collection_name: str, ids: list):
    try:
        # 連接到 MongoDB
        client = MongoClient('mongodb://localhost:27017/')  # 替換為您的 MongoDB 連接字串
        db = client[db_name]  # 選擇資料庫
        collection = db[collection_name]  # 選擇集合（表）

        user_input = ""

        # 提取指定的資料
        for restaurant in collection.find({"_id": {"$in": ids}}):
            restaurant_id = restaurant.get('_id')  # 提取id
            comments = restaurant.get('comments_Info')  # 提取評論資料

            user_input += f"餐廳ID: {restaurant_id}\n"

            if isinstance(comments, list):
                num_comments = len(comments)
                user_input += f"評論數量: {num_comments}\n"

                for item in comments:
                    commenter_name = item.get('commenter_name')
                    comment_star_point = item.get('comment_star_point')
                    comment_content = item.get('comment_content')
                    comment_date = item.get('comment_date')

                    if isinstance(comment_date, str):
                        comment_date = datetime.fromisoformat(comment_date)

                    if comment_date is not None and comment_date > datetime(2024, 2, 1):
                        user_input += f"評論者: {commenter_name}, 評分: {comment_star_point}, 內容: {comment_content}\n"

            user_input += "\n"

        return user_input
    except Exception as e:
        logging.error(f"MongoDB 錯誤: {e}")
        return ""

def gpt_api_text(prompt: str, user_input: str):
    api_key = os.getenv("gpt_api_key")
    if not api_key:
        logging.error("API 金鑰不存在。")
        return "API 金鑰不存在"

    client = OpenAI(api_key=api_key)

    try:
        completion = client.chat.completions.create(
            temperature=0,
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": prompt},
                {"role": "user", "content": user_input}
            ]
        )
        response_content = completion.choices[0].message.content
        prompt_tokens = completion.usage.prompt_tokens  # 提示 (prompt) token 數
        completion_tokens = completion.usage.completion_tokens  # 回應 (completion) token 數

        # 計算成本（按 1M tokens 計價）
        prompt_cost = (prompt_tokens / 1_000_000) * 0.15  # 0.15 美金 / 1M tokens
        completion_cost = (completion_tokens / 1_000_000) * 0.6  # 0.6 美金 / 1M tokens
        total_cost = prompt_cost + completion_cost

        # 顯示 token 消耗資訊
        print(f"Input Token: {prompt_tokens}")
        print(f"Output Token: {completion_tokens}")
        print(f"總共花費: {total_cost}美金")
        print(f"總共花費: {total_cost * 32.85}台幣")

        return completion.choices[0].message.content
    except Exception as e:
        logging.error(f"API 請求失敗: {e}")
        return "API 請求失敗"

# 使用範例
db_name = 'tripadvisor'
collection_name = 'google_comment_v6'

# 定義要提取的特定 _id 列表
specific_ids = [
    "c1ea8e42-c79e-4c87-888a-b09578319756",  # 這裡填寫要提取的 _id
    
      
    # 可以繼續添加其他的 _id
]

# 獲取評論資料並生成 user_input
user_input = extract_comments_from_mongodb(db_name, collection_name, ids=specific_ids)

# 打印 user_input 以檢查內容
print("User Input:")
print(user_input)

prompt = """ 
請幫我分析user輸入的評論在 Google 評論上的內容，並針對以下重點提供摘要：
1. **餐點口味**（評判標準：請以user的內文推薦指數顯示多少%推薦，請直接顯示數字%。）
2. **環境氛圍**(評判標準：請以所有user的內文資訊顯示多少%推薦，請直接顯示數字%。)
3. **服務品質**(評判標準：請以所有user的內文資訊顯示多少%推薦，請直接顯示數字%。)
4. **價位**（評判標準：總體評價、特色、常見評論）(10個字以內)
5. **關鍵字1**（常見的好評/負評一個關鍵詞）(5個字以內) 
6. **關鍵字2**（常見的好評/負評一個關鍵詞）(5個字以內，如果沒有就空白) 
7. **推薦菜色**（評論中最常被提及的推薦一道菜品）(10個字以內)     
8. **適合客群**(評判標準：適合家庭聚會、情侶約會、商務宴請、慶祝場合等，只要給一個客群類型(4個字以內)) 
9. **總體評價**(評判標準:（1~5分）)
請依照以下範例格式輸出:
   餐廳名稱: 
   餐廳ID:
   總體評價:  
   環境氛圍:
   服務品質:
   餐點口味:
   價位:
   關鍵字1:
   關鍵字2:
   推薦菜色:
   適合客群:
"""

all_responses = []

res = gpt_api_text(prompt=prompt, user_input=user_input)

# 打印 API 回應以檢查格式
print("API Response:")
print(res)

if res and res != "API 請求失敗":
    restaurant_data = []
    restaurants = res.split("\n\n")
    for restaurant in restaurants:
        if restaurant.strip():
            info = {}
            lines = restaurant.split("\n")
            for line in lines:
                if ": " in line:
                    key, value = line.split(": ", 1)
                    info[key.strip()] = value.strip()
            # 確保所有關鍵字段都存在
            if all(key in info for key in ['餐廳名稱', '餐廳ID', '總體評價', '環境氛圍', '服務品質', '餐點口味', '價位', '關鍵字1', '關鍵字2', '推薦菜色', '適合客群']):
                restaurant_data.append(info)

    all_responses.extend(restaurant_data)

print("Parsed Restaurant Data:")
print(all_responses)  # 打印解析後的數據

if all_responses:
    df = pd.DataFrame(all_responses)
    df.columns = [col.lstrip(' -') for col in df.columns]

    # 移除包含 NaN 的行
    df.dropna(how='any', inplace=True)
    
    print(df)

    # 將資料附加到現有的 CSV 檔案中
    df.to_csv("google_Comment_specific_0304.csv", mode='a', index=False, header=not os.path.exists("google_Comment_specific_0304.csv"), encoding='utf-8-sig')
    print(f"提取的數據已成功寫入 google_Comment_specific_0304.csv")
else:
    print("沒有有效的回應可寫入文件。")
