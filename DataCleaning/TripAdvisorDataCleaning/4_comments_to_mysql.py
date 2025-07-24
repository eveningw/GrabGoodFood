import pymysql
import pandas as pd
import uuid
import math

#連結到雲端MySQL
def connect_to_mysql(host='35.229.247.71', port=3306, user='root', passwd='bonnie', db='GrabGoodFood', charset='utf8mb4'):
    conn = pymysql.connect(host=host, port=port, user=user, passwd=passwd, db=db, charset=charset)
    return conn

def transfer_data_from_csv(csv_filepath, mysql_conn):
    cursor = mysql_conn.cursor()

    try:
        # 讀取 CSV 檔案
        df = pd.read_csv(csv_filepath)

        total_records = 0
        inserted_records = 0

        # 逐行處理 DataFrame 中的資料
        for index, row in df.iterrows():
            total_records += 1

            # 從 DataFrame 的每一行中提取資料。
            # 注意：這裡的欄位名稱要和 CSV 檔案中的欄位名稱完全一致 (參考第一張圖)
            
            Restaurant_Name = row['餐廳名稱']
            Restaurant_ID = row['餐廳ID']  # CSV 中的餐廳 ID
            Avg_Rating = row['總體評價']
            Env_Quality = row['環境氛圍']
            Service_Quality = row['服務品質']
            Dish_Quality = row['餐點口味']
            Price_Des = row['價位']
            Keyword1 = row['關鍵字1']
            Keyword2 = row['關鍵字2']
            Recommended_Dish = row['推薦菜色']
            Suitable_Customer = row['適合客群']

            # 使用 pd.isna() 檢查並替換為 None
            if pd.isna(Restaurant_Name) or (isinstance(Restaurant_Name, str) and Restaurant_Name.strip() == ''):
                Restaurant_Name = None  # 或 "Unknown"，如果資料庫欄位不允許 NULL
            if pd.isna(Restaurant_ID) or (isinstance(Restaurant_ID, str) and Restaurant_ID.strip() == ''):
                Restaurant_ID = None
            if pd.isna(Avg_Rating):
                Avg_Rating = None  # 對於數值型，如果允許 NULL，則使用 None
            if pd.isna(Env_Quality):
                Env_Quality = None
            if pd.isna(Service_Quality):
                Service_Quality = None
            if pd.isna(Dish_Quality):
                Dish_Quality = None
            if pd.isna(Price_Des):
                Price_Des = None
            if pd.isna(Keyword1) or (isinstance(Keyword1, str) and Keyword1.strip() == ''):
                Keyword1 = None
            if pd.isna(Keyword2) or (isinstance(Keyword2, str) and Keyword2.strip() == ''):
                Keyword2 = None
            if pd.isna(Recommended_Dish) or (isinstance(Recommended_Dish, str) and Recommended_Dish.strip() == ''):
                Recommended_Dish = None
            if pd.isna(Suitable_Customer) or (isinstance(Suitable_Customer, str) and Suitable_Customer.strip() == ''):
                Suitable_Customer = None
            



            # 打印待插入的資料 (用於除錯)
            print(f"Inserting: {Restaurant_Name}, {Restaurant_ID}, {Avg_Rating}, {Env_Quality}, {Service_Quality}, {Dish_Quality}, {Price_Des}, {Keyword1}, {Keyword2}, {Recommended_Dish}, {Suitable_Customer}")

            # 使用 INSERT IGNORE 來避免重複插入 (根據您的需求，可能需要調整資料表名稱和欄位名稱)
            insert_query = """
            INSERT IGNORE INTO Comments_Analysis (
                Restaurant_Name, Restaurant_ID, Avg_Rating, Env_Quality, Service_Quality, Dish_Quality, Price_Des, Keyword1, Keyword2, Recommended_Dish, Suitable_Customer
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(insert_query, (
                Restaurant_Name, Restaurant_ID, Avg_Rating, Env_Quality, Service_Quality, Dish_Quality, Price_Des, Keyword1, Keyword2, Recommended_Dish, Suitable_Customer
            ))

            if cursor.rowcount > 0:
                inserted_records += 1

        mysql_conn.commit()
        print(f'Total records read from CSV: {total_records}')
        print(f'Total records successfully inserted into MySQL: {inserted_records}')

    except Exception as e:
        print(f"Error during data transfer: {e}")
        # 可以在這裡添加更詳細的錯誤處理，例如回滾事務
        # mysql_conn.rollback()
    finally:
        if cursor:
            cursor.close()


# 主程式 (請根據您的實際情況修改 csv_filepath)
if __name__ == '__main__':
    mysql_conn = connect_to_mysql()  # 建立 MySQL 連接
    csv_filepath = "google_Comment_0304.csv"  # 替換為您的 CSV 檔案路徑

    try:
        transfer_data_from_csv(csv_filepath, mysql_conn)
    except Exception as e:
        print(f'Error: {e}')
    finally:
        if mysql_conn:
            mysql_conn.close()
