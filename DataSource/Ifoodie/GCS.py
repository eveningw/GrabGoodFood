import os
from google.cloud import storage
from google.oauth2.service_account import Credentials
from pathlib import Path

# 設定 GCS 認證
GCS_CREDENTIALS_FILE_PATH = "artifacts-registry-user.json"
CREDENTIALS = Credentials.from_service_account_file(GCS_CREDENTIALS_FILE_PATH)

# 建立 GCS 客戶端
client = storage.Client(credentials=CREDENTIALS)

# GCS 參數
bucket_name = "tir104-alina"  # 替換為你的 GCS Bucket 名稱


def upload_source_to_gcs(local_file_path):
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
        
def upload_cleaning_to_gcs(local_file_path):
    """將本機檔案上傳至 GCS"""
    # 取得檔案名稱
    try:

        filename = os.path.basename(local_file_path)
        blob_name = f"cleaning_output/{filename}" # 替換為要上傳或下載的檔案名稱
        bucket = client.bucket(bucket_name) 
        blob = bucket.blob(blob_name)
        blob.upload_from_filename(local_file_path) # 本機要上傳的檔案
        print(f"檔案 {local_file_path} 已成功上傳至 GCS {bucket_name}/{blob_name}")
    except Exception as e:
        print(f"錯誤: {e}")