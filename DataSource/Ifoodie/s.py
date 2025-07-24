import mysql.connector
import pandas as pd

# 連接 MySQL 資料庫
conn = mysql.connector.connect(
    host="35.229.247.71",
    user="root",
    password="bonnie",
    database="GrabGoodFood"
)
cursor = conn.cursor()

# 檢查資料表是否存在，若不存在則創建表格
cursor.execute("SHOW TABLES LIKE 'ifoodie_Restaurants_Info_sql';")
result = cursor.fetchone()

if not result:
    # 創建資料表，將 Restaurant_Name 設為主鍵
    cursor.execute('''
    CREATE TABLE ifoodie_Restaurants_Info_sql (
        Restaurant_Name VARCHAR(100) PRIMARY KEY,
        FullAddress VARCHAR(255),
        Average_Spend VARCHAR(50),
        CreatedAt DATETIME,
        ModifiedAt DATETIME,
        Rating FLOAT,
        Phone_Number VARCHAR(20)
    );
    ''')
    print("表格 'ifoodie_Restaurants_Info_sql' 已創建")

# 讀取 CSV 文件
df = pd.read_csv('ifoodie_exported_data.csv')

# 處理缺失值，將空字串或 NaN 轉換為 None (在 MySQL 中對應於 NULL)
df = df.apply(lambda col: col.map(lambda x: None if pd.isna(x) or x == '' else x))

# 這裡進行欄位名稱對應
df = df.rename(columns={
    '_id': '_id',
    '餐廳名稱': 'Restaurant_Name',
    '地址': 'FullAddress',
    '均消': 'Average_Spend',
    '建立時間': 'CreatedAt',
    '更新時間': 'ModifiedAt',
    '評分': 'Rating',
    '電話': 'Phone_Number'
})

# 確保所有缺失的欄位設為 None（這樣 MySQL 插入時會是 NULL）
for col in ['Restaurant_Name', 'FullAddress', 'Average_Spend', 'CreatedAt', 'ModifiedAt', 'Rating', 'Phone_Number']:
    if col not in df.columns:
        df[col] = None

# 確保在插入數據時，列數一致，並且不要錯過任何必要的欄位
data_to_insert = []
for i, row in df.iterrows():
    data_to_insert.append((
        row['Restaurant_Name'],
        row.get('FullAddress', None),
        row.get('Average_Spend', None),
        row.get('CreatedAt', None),
        row.get('ModifiedAt', None),
        row.get('Rating', None),
        row.get('Phone_Number', None)
    ))

# 批次插入資料
cursor.executemany('''
    INSERT INTO ifoodie_Restaurants_Info_sql (Restaurant_Name, FullAddress, Average_Spend, CreatedAt, ModifiedAt, Rating, Phone_Number)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
''', data_to_insert)

# 提交更改並關閉連接
conn.commit()
cursor.close()
conn.close()
