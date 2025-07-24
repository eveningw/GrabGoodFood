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

def upload_to_gcs(local_file_path, file_type):
    """將本機檔案上傳至 GCS"""
   
    try:

        filename = os.path.basename(local_file_path)

         # 取得檔案名稱
        blob_name = filename
        if file_type == "cleaning":
            blob_name = f"cleaning_output/{filename}" # 上傳至 GCS 的檔案名稱
        elif file_type == "source":
            blob_name = f"source_output/{filename}" # 上傳至 GCS 的檔案名稱

        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.upload_from_filename(local_file_path)
        print(f"檔案 {local_file_path} 已成功上傳至 GCS {bucket_name}/{blob_name}")
    except Exception as e:
        print(f"錯誤: {e}")

def download_from_gcs(local_file_path):
    """從 GCS 下載檔案至本機"""
    filename = os.path.basename(local_file_path)

    blob_name = f"cleaning_output/{filename}"
    # download_path = "downloaded/downloaded_" + blob_name  # 下載後的本機檔案名稱
    bucket = client.bucket(bucket_name)

    # 遠端檔案名稱
    blob = bucket.blob(blob_name)

    # 取得當前 Python 檔案所在的資料夾(Utils )
    py_base_dir = os.path.dirname(os.path.abspath(__file__))

    # 從 Utils 到 GoogleDataCrawler 根目錄
    project_root = os.path.dirname(py_base_dir)

    # 相對於該程式的 "downloaded" 資料夾
    download_folder = os.path.join(project_root, "downloaded")
    download_path = os.path.join(download_folder, os.path.basename(blob_name))
    
    # 自動建立資料夾（如果不存在）
    os.makedirs(download_folder, exist_ok=True)
    blob.download_to_filename(download_path)
    print(f"檔案 {blob_name} 已從 GCS 下載至 {download_path}")

