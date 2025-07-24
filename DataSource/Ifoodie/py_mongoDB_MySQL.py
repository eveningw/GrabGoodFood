from pymongo import MongoClient
import mysql.connector

# 連接 MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['restaurant_db']  # 指定 MongoDB 資料庫名稱
collection = db['restaurants']  # 指定 MongoDB 集合名稱

# client = MongoClient('mongodb://34.81.181.216:27200')
# db = client['GrabGoodFood_ETL']  # 指定 MongoDB 資料庫名稱
# collection = db['ifoodie_Restaurants_Info']  # 指定 MongoDB 集合名稱

# 連接 MySQL 資料庫
conn = mysql.connector.connect(
    host="35.229.247.71",  # 替換為你的 MySQL 主機
    user="root",  # 替換為你的 MySQL 用戶名
    password="bonnie",  # 替換為你的 MySQL 密碼
    database="GrabGoodFood"  # 替換為你的資料庫名稱
)
cursor = conn.cursor()

# 創建資料表
create_table_query = """
CREATE TABLE IF NOT EXISTS ifoodie_Restaurants_Info_sql (
    id INT AUTO_INCREMENT PRIMARY KEY,
    Restaurant_Name VARCHAR(100),
    Restaurant_Type VARCHAR(20),
    Rating FLOAT,
    Total_Comment INT,
    City VARCHAR(3),
    District VARCHAR(3),
    FullAddress VARCHAR(100),
    Phone_Number VARCHAR(20),
    CreatedAt DATETIME,
    ModifiedAt DATETIME,
    Source VARCHAR(20)
);
"""
cursor.execute(create_table_query)

# MongoDB 資料欄位名稱映射到 MySQL
field_mapping = {
    "Restaurant_Name": "餐廳名稱",
    "Rating": "評分",
    "Phone_Number": "電話",
    "CreatedAt": "建立時間",
    "ModifiedAt": "更新時間",
    "Restaurant_Type": "餐廳類型",
    "Total_Comment": "總評論數",
    "City": "城市",
    "District": "區域",
    "FullAddress": "地址",
    "Source": "來源"
}

# 從 MongoDB 讀取資料
mongo_data = collection.find()

# 插入資料到 MySQL
for document in mongo_data:
    # 根據映射字典將 MongoDB 的欄位名稱轉換為相應的 MySQL 欄位名稱
    restaurant_name = document.get(field_mapping["Restaurant_Name"])
    rating = document.get(field_mapping["Rating"])
    phone_number = document.get(field_mapping["Phone_Number"])
    created_at = document.get(field_mapping["CreatedAt"])
    modified_at = document.get(field_mapping["ModifiedAt"])

    # 插入資料到 MySQL 資料表，注意省略了 Restaurant_ID，讓 MySQL 自動生成
    cursor.execute(
        "INSERT INTO ifoodie_Restaurants_Info_sql (Restaurant_Name, Rating, Phone_Number, CreatedAt, ModifiedAt) VALUES (%s, %s, %s, %s, %s)",
        (restaurant_name, rating, phone_number, created_at, modified_at)
    )

# 提交更改並關閉連接
conn.commit()
cursor.close()
conn.close()

print("資料成功插入 MySQL！")