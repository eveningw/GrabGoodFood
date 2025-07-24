import requests
from bs4 import BeautifulSoup
import csv
import re
from datetime import datetime
import os
import pandas as pd
from pymongo import MongoClient
from google.cloud import storage
from google.oauth2.service_account import Credentials
from pathlib import Path

from GCS import upload_cleaning_to_gcs, upload_source_to_gcs

# 設定起始頁面 URL
base_url = 'https://ifoodie.tw/explore/台北市/list?page='  # 替換成實際的 URL
page_num = 1  # 從第 1 頁開始

# 獲取當前時間，作為資料建立時間
current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# 設定輸出的資料夾路徑
output_folder = r'C:\Users\Tibame\Desktop\test'  # 指定資料夾
if not os.path.exists(output_folder):
    os.makedirs(output_folder)

file_name = 'ifoodie_restaurant_Info_taipei.csv'
file_path = os.path.join(output_folder, file_name)

# 打開 CSV 文件並準備寫入資料
with open(file_path, mode='a', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    
    # 只寫入標題列（如果 CSV 文件是空的）
    if file.tell() == 0:  # 檢查文件是否為空
        writer.writerow(['餐廳名稱', '均消', '評分', '地址', '電話', '建立時間', '更新時間'])

    while True:
        # 組合頁面 URL
        url = f"{base_url}{page_num}"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        response = requests.get(url, headers=headers)

        # 確保頁面被成功抓取
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')

            # 假設餐廳名稱、均消、星級數、地址在 <a> 和 <div> 標籤中
            restaurant_name_tags = soup.find_all('a', class_=re.compile('.*title-text.*'))
            avg_price_tags = soup.find_all('div', class_="jsx-1794446983 avg-price")
            rating_tags = soup.find_all('div', class_="jsx-2373119553 text") 
            address_tags = soup.find_all('div', class_=re.compile('.*address-row.*')) 

            # 走過每個餐廳並寫入 CSV 文件
            for name_tag, avg_price_tag, rating_tag, address_tag in zip(restaurant_name_tags, avg_price_tags, rating_tags, address_tags):
                restaurant_name = name_tag.get_text().strip() if name_tag else '無名稱'
                rating = rating_tag.get_text().strip() if rating_tag else '無評分'
                address = address_tag.get_text().strip() if address_tag else '無地址'

                # 處理均消資料
                avg_price = avg_price_tag.get_text().strip() if avg_price_tag else '無價格'
                avg_price = re.sub(r'·\s*均消\s*', '', avg_price)  # 去除開頭的 "· 均消"
                avg_price = avg_price.replace(' ', '')  # 去除中間的多餘空格

                # 從每個餐廳的詳細頁面抓取電話號碼
                restaurant_url = name_tag['href'] if name_tag and 'href' in name_tag.attrs else ''
                phone_number = '無電話'

                if restaurant_url:
                    # 確保 URL 是完整的
                    if not restaurant_url.startswith('https'):
                        restaurant_url = 'https://ifoodie.tw' + restaurant_url
                    
                    # 進入餐廳詳細頁面
                    detail_response = requests.get(restaurant_url, headers=headers)
                    if detail_response.status_code == 200:
                        detail_soup = BeautifulSoup(detail_response.text, 'html.parser')
                        phone_tag = detail_soup.find('a', href=re.compile('tel:'))  # 找到 href 以 tel: 開頭的 a 標籤
                        if phone_tag:
                            phone_number = phone_tag.get_text().strip()  # 提取電話號碼，並保持原始格式

                # 更新時間為當前時間
                updated_time = current_time

                # 寫入 CSV 資料
                writer.writerow([restaurant_name, avg_price, rating, address, phone_number, current_time, updated_time])

            print(f"已抓取第 {page_num} 頁的資料")

            # 檢查是否有「下一頁」鏈接
            next_page_link = soup.find('a', href=re.compile('.*page=\d+.*')) 
            if next_page_link:
                page_num += 1  # 若有下一頁，則抓取下一頁
            else:
                break  # 若沒有下一頁，則停止抓取
        else:
            print(f"無法抓取第 {page_num} 頁")
            break  # 如果頁面無法正常抓取，則停止抓取


####################################################
    
upload_source_to_gcs(file_path)

####################################################

# 在抓取完資料後，將資料匯入 MongoDB
# 連接到 MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['restaurant_db']  # 指定 MongoDB 資料庫名稱
collection = db['restaurants']  # 指定 MongoDB 集合名稱

# client = MongoClient('mongodb://34.81.181.216:27200')
# db = client['GrabGoodFood_ETL']  # 指定 MongoDB 資料庫名稱
# collection = db['ifoodie_Restaurants_Info']  # 指定 MongoDB 集合名稱

# 讀取 CSV 文件並轉換為 DataFrame
df = pd.read_csv(file_path)

# 清理欄位名稱（去除多餘的空格）
df.columns = df.columns.str.strip()

# 確保所有欄位名稱都是字串
df.columns = df.columns.astype(str)

# 過濾掉無名稱的資料（餐廳名稱為無名稱的資料無效）
df = df[df['餐廳名稱'] != '無名稱']
df = df[df['餐廳名稱'].notnull()]  # 去除空欄位

# 4. 去除重複的餐廳名稱，只保留最後一次出現的資料
df = df.drop_duplicates(subset='餐廳名稱', keep='last')

# 5. 清空 MongoDB 集合中的舊資料
collection.delete_many({})

# 6. 處理數值型欄位，把它們轉換為字串，避免 MongoDB 出現無效資料
df = df.apply(lambda col: col.map(lambda x: str(x) if pd.notnull(x) else '無資料'))

# 將 DataFrame 轉換為字典列表
data_to_insert = df.to_dict(orient='records')

# 插入資料到 MongoDB 集合，使用 upsert 來避免重複資料
for record in data_to_insert:
    # 確保 '餐廳名稱' 這一欄存在於 record 中
    if '餐廳名稱' not in record:
        print("錯誤: 餐廳名稱在資料中找不到，跳過這條記錄")
        continue
    
    filter_condition = {'餐廳名稱': record['餐廳名稱']}
    
    # 使用 upsert 操作，若餐廳名稱已存在則更新資料，否則插入新資料
    result = collection.update_one(filter_condition, {'$set': record}, upsert=True)

    # 輸出插入結果
    if result.matched_count > 0:
        print(f"更新資料: {record['餐廳名稱']}")
    elif result.upserted_id:
        print(f"插入新資料: {record['餐廳名稱']}")

# 顯示成功消息
print(f"成功插入或更新 {len(data_to_insert)} 筆資料")

# 匯出資料到指定資料夾
# 設定匯出路徑
output_export_path = os.path.join(output_folder, 'ifoodie_exported_data.csv')

# 從 MongoDB 讀取資料
exported_data = list(collection.find())

# 確保所有文檔都有一致的欄位，並將其轉換為 DataFrame
if exported_data:
    # 提取所有欄位名稱（會自動去除無效欄位）
    columns = list(exported_data[0].keys())

    # 只保留一致的欄位名稱
    cleaned_data = [{key: record.get(key, '無資料') for key in columns} for record in exported_data]
    
    # 將處理過的資料轉換為 DataFrame
    exported_df = pd.DataFrame(cleaned_data, columns=columns)
    
    # 將 DataFrame 轉換為 CSV
    exported_df.to_csv(output_export_path, index=False, encoding='utf-8')
    print(f"資料已成功匯出到 {output_export_path}")
else:
    print("MongoDB 中沒有資料")



###################################################

upload_cleaning_to_gcs(output_export_path)

####################################################