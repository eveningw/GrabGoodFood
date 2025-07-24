import pandas as pd
from pymongo import MongoClient
from prefect import task, flow
import inspect



@task
def read_csv(file_path):
    """讀取 CSV 檔案並轉換為 DataFrame"""
    return pd.read_csv(file_path)

@task
def insert_to_mongo(df, db_name, collection_name):
    """將 DataFrame 插入 MongoDB，若電話欄位相同則更新資料"""
    with MongoClient("mongodb://host.docker.internal:27017/") as client:
        db = client[db_name]
        collection = db[collection_name]

        for _, row in df.iterrows():
            collection.update_one(
                {"電話": row["電話"]},  # 依據「電話」欄位來比對
                {"$set": row.to_dict()},  # 更新整筆資料
                upsert=True  # 若無則插入
            )
    
    return "Data inserted successfully!"




