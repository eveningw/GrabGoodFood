from pymongo import MongoClient
import pymysql
import uuid
import math

# 雲端mongoDB
def connect_to_mongodb_cloud(uri='mongodb://34.81.181.216:27200', db_name='GrabGoodFood_ETL'): 
    client = MongoClient(uri)
    return client[db_name]

# 地端mongoDB
def connect_to_mongodb_local(uri='mongodb://localhost:27017/', db_name='tripadvisor'): 
    client = MongoClient(uri)
    return client[db_name]


def connect_to_mysql(host='35.229.247.71', port=3306, user='root', passwd='bonnie', db='GrabGoodFood', charset='utf8mb4'):
    conn = pymysql.connect(host=host, port=port, user=user, passwd=passwd, db=db, charset=charset)
    return conn

def transfer_data(mongo_db, mysql_conn):
    cursor = mysql_conn.cursor()
    
    # 雲端MongoDB 中的collection名為 Tripadvisor
    #mongo_collection = mongo_db['Tripadvisor']

    # 地端MongoDB 中的collection名為 restaurant3
    mongo_collection = mongo_db['restaurants3']

    total_records = 0  # 總讀取的記錄數
    inserted_records = 0  # 成功插入的記錄數
    
    # 讀取 MongoDB 中的資料
    for document in mongo_collection.find():
        total_records += 1  # 每讀取一條記錄，計數加 1

        # 根據您的資料結構選擇要插入的欄位
        # 生成 UUID
        Restaurant_ID = str(uuid.uuid4())
        Restaurant_Name = document.get('Restaurant_Name')
        Restaurant_Type = document.get('Restaurant_Type')
        Rating = document.get('Rating')
        Total_Comment = document.get('Total_Comment')
        City = document.get('City')
        FullAddress = document.get('FullAddress')
        District = document.get('District')  # 新增 District 欄位
        # 獲取 Phone_Number
        Phone_Number = document.get('Phone_Number')

        # 檢查 Phone_Number 是否為 None 或空字符串，若是則跳過該條記錄
        if Phone_Number is None or Phone_Number.strip() == '':
            print(f"Skipping insertion for Restaurant '{Restaurant_Name}' due to missing Phone_Number.")
            continue  # 跳過插入

        # 檢查 Rating 和 Total_Comment 是否為 NaN，並提供預設值
        if Rating is None or (isinstance(Rating, float) and math.isnan(Rating)):
            Rating = 0  # 或其他合適的預設值
        if Total_Comment is None or (isinstance(Total_Comment, float) and math.isnan(Total_Comment)):
            Total_Comment = 0  # 或其他合適的預設值

        # 檢查 Restaurant_Type 是否為 NaN，並提供預設值
        if Restaurant_Type is None or (isinstance(Restaurant_Type, str) and Restaurant_Type.strip() == '') or (isinstance(Restaurant_Type, float) and math.isnan(Restaurant_Type)):
            Restaurant_Type = "Unknown"

        # 檢查 City、FullAddress 和 District 是否為 NaN，並提供預設值
        if City is None or (isinstance(City, str) and City.strip() == ''):
            City = "Unknown"
        if FullAddress is None or (isinstance(FullAddress, str) and FullAddress.strip() == ''):
            FullAddress = "Unknown"
        if District is None or (isinstance(District, str) and District.strip() == ''):
            District = "Unknown"

        # 打印待插入的資料以進行調試
        print(f"Inserting: {Restaurant_ID}, {Restaurant_Name}, {Restaurant_Type}, {Rating}, {Total_Comment}, {City}, {FullAddress}, {Phone_Number}, {District}")

        # 使用 INSERT IGNORE 來避免重複插入
        insert_query = """
        INSERT IGNORE INTO Tripadvisor_Restaurant_Info (Restaurant_ID, Restaurant_Name, Restaurant_Type, Rating, Total_Comment, City, FullAddress, Phone_Number, District) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(insert_query, (Restaurant_ID, Restaurant_Name, Restaurant_Type, Rating, Total_Comment, City, FullAddress, Phone_Number, District))
        

        # 檢查是否成功插入
        if cursor.rowcount > 0:
            inserted_records += 1  # 如果插入成功，計數加 1

    mysql_conn.commit()  # 提交更改
    cursor.close()

    return total_records, inserted_records  # 返回總記錄數和成功插入的記錄數

# 主程式
if __name__ == '__main__':
    #mongo_db = connect_to_mongodb_cloud()
    mongo_db = connect_to_mongodb_local()
    mysql_conn = connect_to_mysql()
    
    try:
        total_records, inserted_records = transfer_data(mongo_db, mysql_conn)
        print(f'Total records read from MongoDB: {total_records}')
        print(f'Total records successfully inserted into MySQL: {inserted_records}')
    except Exception as e:
        print(f'Error: {e}')
    finally:
        mysql_conn.close()
