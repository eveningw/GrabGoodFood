# 用來清洗 michelin collection 資料, 並存入 michelin_cleaned collection
# 1. 將電話號碼的 +886 改成 0
# 2. 將 FullAddress 欄位的地址取出, 並新增 City 欄位, 以及將 FullAddress 欄位的地址取出
# 3. 將 type 欄位的價位和餐廳類型分開成 money 和 Restaurant_Type 欄位
# 4. 將清洗後的資料存入 mysql 的 michelin表
# 5. UUID 欄位是為了之後到MySQL要做資料比對用的, 這裡先不用加入

# #建立MySQL Table
#
# CREATE DATABASE `proj_db`; 
# use proj_db;
# create table michelin_cleaned(
# 	Restaurant_Name varchar(500),
#     Phone_Number varchar(20),
#     FullAddress varchar(100),
#     City varchar(3),
#     price_category varchar(5),
#     Restaurant_Type varchar(20)
# );

# select * from michelin_cleaned;

import pandas as pd
import pymongo
import pymysql

dbName= "proj_db"
colName="michelin"
new_colName="michelin_cleaned"

# 每次執行前先清空 michelin_cleaned collection
def Init():
    # 連接到 MongoDB
    client = Get_Client()
    db = client[dbName]
    if new_colName in db.list_collection_names():
        db[new_colName].drop()
        print(f"The collection '{new_colName}' has been dropped.")
    client.close()

# 取得client
def Get_Client() -> pymongo.MongoClient:
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    return client

# 取得 collection
def Get_Collection(dbName=dbName, colName=colName) -> pymongo.collection.Collection:
    client = Get_Client()
    # 檢查client是否存在
    if not client:
        return None # ToDo:存連線失敗到log table 

    # 取得db
    db = client[dbName]

    # 檢查collection是否存在
    if colName not in db.list_collection_names():
        # print(f"The collection '{colName}' does not exist. Creating new collection.")
        db.create_collection(colName)

    #印出doc: #之前處理過了, 這裡沒有欄位會印出NaN 
    # for doc in db[colName].find():
    #     print(doc)

    return db[colName]

# 清空michelin_cleaned collection
Init()


def Clean_Michelin_Collection():
    # 取得collection, 準備用來清洗資料
    michelin_col = Get_Collection(dbName, colName)

    data_list = list(michelin_col.find())
    # print(data_list) #[{},{},{},...]

    # 逐一取出每一筆資料, 進行清洗
    for item in data_list:
        # 1. 將電話號碼的 +886 改成 0
        if "Phone_Number" in item:
            item["Phone_Number"] = item["Phone_Number"].replace("+886 ", "0").replace(" ", "") if item["Phone_Number"] else None

        # 2. 將 FullAddress 欄位的地址取出, 並新增 City 欄位
        if "Taipei" in item["FullAddress"]:
            item["City"] = "臺北市"
        elif "Tainan" in item["FullAddress"]: 
            item["City"] = "臺南"
        else:    
            item["City"] = None   
        # 取出 FullAddress 欄位的地址
        if "FullAddress" in item:
            item["FullAddress"] = item["FullAddress"].split("(")[0].split(",")[0].strip() 

        # 3. 將 type 欄位的價位和餐廳類型分開成 money 和 Restaurant_Type 欄位
        if "type" in item:
            type_split = item["type"].split("·")
            if len(type_split) == 2:
                item["money"] = type_split[0]
                item["Restaurant_Type"] = type_split[1]
            else:
                item["money"] = None
                item["Restaurant_Type"] = None
            del item["type"]

    # df=pd.DataFrame(data_list)
    # print(df)

    # 取得新collection, 準備用來新增清洗資料
    michelin_col_cleaned = Get_Collection(dbName, new_colName)

    # 將清洗後的資料存入MySQL的michelin表
    if data_list:
        michelin_col_cleaned.insert_many(data_list)
        # 連線 MySQL
        Save_Cleaned_Data_To_MySQL(data_list)
        print(f"已成功將清洗後的資料存入 {new_colName}")
        
    else:
        print("無可存入的資料。")


# 將清洗後的資料存入MySQL的michelin表
def Save_Cleaned_Data_To_MySQL(data_list):
    # 連線 MySQL
    conn = pymysql.connect(host='localhost', user='root', password='password', db='proj_db', charset='utf8')
    print('Successfully connected!')
    cursor = conn.cursor()  # 建立游標

    try:
        cursor.execute("delete from michelin_cleaned")  # 清空michelin_cleaned表
        conn.commit()

        val_list = []
        
        for item in data_list:
            # 將清洗後的資料存入 mysql 的 michelin表
            sql = "INSERT INTO michelin_cleaned (Restaurant_Name, Phone_Number, FullAddress, City, price_category, Restaurant_Type) VALUES (%s, %s, %s, %s, %s, %s)"
            val_list.append((item["Restaurant_Name"], 
                                item["Phone_Number"], 
                                item["FullAddress"], 
                                item["City"], 
                                item["money"], 
                                item["Restaurant_Type"]))
            cursor.executemany(sql, val_list)
            conn.commit()
            print(f"已成功將清洗後的資料存入 michelin 表。")     
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()
Clean_Michelin_Collection()


