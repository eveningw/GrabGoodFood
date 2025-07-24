import pandas
import pymongo

#先存一版到mongoDB
# 修改 1veggie2mongo.py的程式碼

dbName= "proj_db"
colName="veggie"

def get_client(dbName= "proj_db"):
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    return client

def get_collection(dbName="proj_db", colName="veggie"):
    client = get_client()
    # 檢查client是否存在
    if not client:
        return None # ToDo:存連線失敗到log table 

    db = client[dbName]

    # 檢查collection是否存在
    if colName not in db.list_collection_names():
        print("The collection does not exist.")
        return None  # ToDo:存colName不存在到log table, 假如非差異更新

    # #印出doc: #有些欄位會印出NaN 
    # for doc in db[colName].find():
    #     print(doc)

    return db[colName]


# 取得collection, 準備把df 用 insert_many() 存入mongodb
veggie_col = get_collection()

df = pandas.read_csv("蔬食餐廳_中文版(OpenData).csv")
print(df)

data_dict = df.to_dict(orient='records') #orient='records' 轉換出的格式接近 JSON，適合存成 JSON 檔

col = get_collection(dbName="proj_db", colName="veggie")



# # 待處理後再存db
# col.insert_many(data_dict)
