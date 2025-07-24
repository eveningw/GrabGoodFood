from prefect import flow, task
from pymongo import MongoClient
import pandas as pd
import csv
import re

#　讀取ind餐廳的csv檔案
@task
def read_csv(file_path):
    return pd.read_csv(file_path)

# 將檔案存入mongodb,如果相同電話就不會新增避免資料重複。
@task
def insert_to_mongo(df, db_name, collection_name):
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

# 用於從 MongoDB 讀取數據，並轉換成 Pandas DataFrame。
@task
def fetch_from_mongo(db_name, collection_name):
    with MongoClient("mongodb://host.docker.internal:27017/") as client:
        db = client[db_name]
        collection = db[collection_name]
        cursor = collection.find({})
        df = pd.DataFrame(cursor)
        if "_id" in df.columns:
            df.drop("_id", axis=1, inplace=True)
    return df

# 將 Pandas DataFrame 儲存為 CSV 檔案。
@task
def save_to_csv(df, file_path):
    df.to_csv(file_path, index=False, encoding="utf-8-sig")
    return "CSV Exported Successfully!"

# 清理非臺北市相關的資料，並進行刪除英文、去除特定欄位、移除特殊符號。
@task
def clean_data(file_path):
    data = []
    with open(file_path, "r", encoding="utf-8-sig") as f:
        csv_reader = csv.reader(f)
        for row in csv_reader:
            data.append(row)
    
    cleaned_data = []
    for row in data:
        if "臺北市Taipei City " not in row:
            continue
        cleaned_row = [re.sub(r"[a-zA-Z]", "", item).strip() for item in row]
        if len(cleaned_row) > 3:
            del cleaned_row[3:5]
        cleaned_row = [item.replace("?", "") for item in cleaned_row]
        cleaned_row[0] = re.sub(r"[^\u4e00-\u9fff0-9]", "", cleaned_row[0])
        cleaned_data.append(cleaned_row)
    
    return cleaned_data

# 將營業時間（Operating Hours）拆分為多個時段，方便後續處理或存入資料庫。
@task
def split_operating_hours(data):
    result = []
    for row in data:
        if "、" in row[4]:
            time_slots = row[4].split("、")
        else:
            time_slots = [row[4]]
        result.append(time_slots)
    return result

# 處理營業時間的格式，將不同的時間資訊拆分成結構化的格式，方便後續分析或存入資料庫。
@task
def process_time_slots(data):
    processed_data = []
    for row in data:
        temp_result = []
        for item in row:
            item = re.sub(r'(平日|假日)(:)', r'|\1\2|', item)
            item = re.sub(r'(週[一二三四五六日])', r'|\1|', item)
            item = re.sub(r'公休', r'|公休|', item)
            item = re.sub(r'(\d{2}:\d{2})[~-](\d{2}:\d{2})', r'\1|\2', item)
            temp_result.extend([x.strip() for x in item.split('|') if x.strip()])
        processed_data.append(temp_result)
    return processed_data

@flow
def main_flow(csv_path, mongo_db, mongo_collection, output_csv):
    df = read_csv(csv_path)
    insert_to_mongo(df, mongo_db, mongo_collection)
    df = fetch_from_mongo(mongo_db, mongo_collection)
    save_to_csv(df, output_csv)
    cleaned_data = clean_data(output_csv)
    split_hours = split_operating_hours(cleaned_data)
    processed_time = process_time_slots(split_hours)
    return processed_time



if __name__ == "__main__":
    
    result = main_flow(
        csv_path="/workspaces/prefect-demo/Ind_re.csv",
        mongo_db="test",
        mongo_collection="my_ind_re",
        output_csv="ind_mango.csv"
    )
    print(result)
    print("成功")