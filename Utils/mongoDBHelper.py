import pymongo

def get_mongo_connection():
    """建立並返回 MongoDB 連線"""
    try:
        client = pymongo.MongoClient("mongodb://localhost:27017/")
        db = client["test"]  # 指定資料庫
        print("成功連接 MongoDB")
        return client, db
    except pymongo.errors.ConnectionFailure as e:
        print(f"MongoDB 連線失敗: {e}")
        return None, None
