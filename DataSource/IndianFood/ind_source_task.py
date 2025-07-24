from prefect import flow, task
from pymongo import MongoClient
import pandas as pd

@task
def read_csv(file_path):
    """讀取 CSV 檔案並回傳 DataFrame"""
    return pd.read_csv(file_path)

@task
def insert_to_mongo(df, db_name, collection_name):
    """將 DataFrame 插入到 MongoDB，若電話相同則更新，否則新增"""
    with MongoClient("mongodb://host.docker.internal:27017/") as client:
        db = client[db_name]
        collection = db[collection_name]

        for _, row in df.iterrows():
            collection.update_one(
                {"電話": row["電話"]},  # 找到相同「電話」的資料
                {"$set": row.to_dict()},  # 更新整個資料
                upsert=True  # 找不到就新增
            )
    return "Data inserted successfully!"

@flow
def csv_to_mongo_flow(file_path: str, db_name: str, collection_name: str):
    """完整的 Prefect Flow：讀取 CSV 並插入到 MongoDB"""
    df = read_csv(file_path)
    result = insert_to_mongo(df, db_name, collection_name)
    print(result)

# 執行 Flow
if __name__ == "__main__":
    csv_to_mongo_flow("/workspaces/prefect-demo/Ind_re.csv", "test", "my_ind_re")


