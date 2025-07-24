import pandas as pd
import pymongo

dbName = "proj_db"
colName = "michelin"
new_colName = "michelin_cleaned"

# 每次執行前先清空 michelin_cleaned collection
def Init():
    client = Get_Client()
    db = client[dbName]
    if new_colName in db.list_collection_names():
        db[new_colName].drop()
        print(f"The collection '{new_colName}' has been dropped.")
    client.close()

# 取得 MongoDB client
def Get_Client() -> pymongo.MongoClient:
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    return client

# 取得 MongoDB collection
def Get_Collection(dbName=dbName, colName=colName) -> pymongo.collection.Collection:
    client = Get_Client()
    if not client:
        return None  # ToDo: 存連線失敗到 log table

    db = client[dbName]

    # 確保 collection 存在
    if colName not in db.list_collection_names():
        print(f"The collection '{colName}' does not exist. Creating new collection.")
        db.create_collection(colName)

    return db[colName]

# 初始化，清空目標 collection
Init()

# 取得 collection，準備清洗資料
michelin_col = Get_Collection(dbName, colName)

# 改用 Aggregation Pipeline 直接在 MongoDB 內部清洗數據
data_list = list(michelin_col.aggregate([
    # 1️ 清洗電話號碼：+886 轉 0，移除空格
    {"$set": {
        "Phone_Number": {
            "$replaceAll": {
                "input": {
                    "$replaceAll": {
                        "input": {"$replaceAll": {"input": "$Phone_Number", "find": "+886 ", "replacement": "0"}},
                        "find": " ",
                        "replacement": ""
                    }
                },
                "find": "-",
                "replacement": ""
            }
        }
    }},
    
    # 2️ 從 FullAddress 中提取城市名稱
    {"$set": {
        "City": {
            "$switch": {
                "branches": [
                    {"case": {"$regexMatch": {"input": "$FullAddress", "regex": "Taipei"}}, "then": "臺北市"},
                    {"case": {"$regexMatch": {"input": "$FullAddress", "regex": "Tainan"}}, "then": "臺南"}
                ],
                "default": None
            }
        }
    }},

    # 3️ 只保留 FullAddress 內的地址，去掉括號與逗號後的內容
    {"$set": {
        "FullAddress": {
            "$arrayElemAt": [{"$split": ["$FullAddress", "("]}, 0]
        }
    }},
    {"$set": {
        "FullAddress": {
            "$arrayElemAt": [{"$split": ["$FullAddress", ","]}, 0]
        }
    }},

    # 4️ 拆分 type欄位，拆成 money和 Restaurant_Type
    {"$set": {
        "money": {"$arrayElemAt": [{"$split": ["$type", "·"]}, 0]},
        "Restaurant_Type": {"$arrayElemAt": [{"$split": ["$type", "·"]}, 1]}
    }},
    {"$unset": "type"},  # 刪除原本 type欄位

    # 5 移除 _id，避免 insert_many()報錯
    {"$unset": "_id"}
]))

# 刪除Python端的type轉換（MongoDB已處理）
# 舊處理邏輯已不需要：
# if "type" in item:
#     item["money"] = item["type"].split("·")[0]
#     item["Restaurant_Type"] = item["type"].split("·")[1]
#     del item["type"]

# 取得新collection，準備存清洗後的資料
michelin_col_cleaned = Get_Collection(dbName, new_colName)

if michelin_col_cleaned is None:
    print(f"{new_colName} does not exist.")
    exit()
else:
    michelin_col_cleaned.insert_many(data_list)  # 直接寫入清洗後的資料

print("Data cleaned and inserted into michelin_cleaned.")
