import pandas as pd
from pymongo import MongoClient
import os
from google.cloud import storage
from google.oauth2.service_account import Credentials
from pathlib import Path

# 設定 GCS 認證
GCS_CREDENTIALS_FILE_PATH = "tripadvisor_mongo/artifacts-registry-user.json"
CREDENTIALS = Credentials.from_service_account_file(GCS_CREDENTIALS_FILE_PATH)

# 建立 GCS 客戶端
client = storage.Client(credentials=CREDENTIALS)

# GCS 參數
bucket_name = "tir104-alina"  # 替換為你的 GCS Bucket 名稱


def upload_to_gcs(local_file_path):
    """將本機檔案上傳至 GCS"""
    # 取得檔案名稱
    try:

        filename = os.path.basename(local_file_path)
        blob_name = f"source_output/{filename}" # 替換為要上傳或下載的檔案名稱
        bucket = client.bucket(bucket_name) 
        blob = bucket.blob(blob_name)
        blob.upload_from_filename(local_file_path) # 本機要上傳的檔案
        print(f"檔案 {local_file_path} 已成功上傳至 GCS {bucket_name}/{blob_name}")
    except Exception as e:
        print(f"錯誤: {e}")

# upload_to_gcs("tripadvisor_restaurants - dataset_tripadvisor_2025-01-26_15-47-29-918_traditional_chinese.csv")


#讀取 CSV 檔案
df = pd.read_csv("tripadvisor_restaurants - dataset_tripadvisor_2025-01-26_15-47-29-918_traditional_chinese.csv")

# 連接到 MongoDB
# 雲端mongoDB
# client = MongoClient('mongodb://34.81.181.216:27200')  
# db = client['GrabGoodFood_ETL']  # 選擇資料庫
# collection = db['Tripadvisor']  # 選擇集合（表）

# 地端mongoDB
client = MongoClient('mongodb://localhost:27017/')  
db = client['tripadvisor']  # 選擇資料庫
collection = db['restaurants3']  # 選擇集合（表）

# 將 DataFrame 轉換為字典格式並存入 MongoDB
collection.insert_many(df.to_dict('records'))

# 關閉連接
client.close()





