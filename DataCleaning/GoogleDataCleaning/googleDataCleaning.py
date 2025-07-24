from datetime import datetime
from Utils.googleExtractAddressHelper import extract_address
from Utils.googleAIHelper import execute_ai_model, get_ai_response
from Utils.mySqlHelper import get_cloud_connection, get_connection
from Utils.mongoDBHelper import get_cloud_mongo_connection, get_mongo_connection
import uuid
import json 
import re
from DataModels.open_time_model import Open_Time_Info
from DataModels.comment_info_model import comment_info
from dateutil.relativedelta import relativedelta #自動處理年月份
import pandas as pd

from Utils.saveDataFrameHelper import save_dataframe


# 取得Mongodb 原始資料
def get_source_data():
    client, db = get_mongo_connection() 
    collection = db["google_taipei_restaurant_data"]
    
    results = list(collection.find())
    print("取得mongo原始資料")

    client.close()

    return results

# 取得價格等級
def get_price_level(price_str):
    if not price_str:
        return None
    
    price_str = price_str.strip().replace(',', '')
    
    # $ 符號價格分類
    if price_str == "$":
        return 0
    elif price_str == "$$":
        return 1
    elif price_str == "$$$":
        return 2
    elif "超過" in price_str:
        return 2
    
    # 價格區間計算平均
    range_match = re.match(r"\$(\d+)[-~](\d+)", price_str)
    if range_match:
        low, high = map(int, range_match.groups())
        avg_price = (low + high) // 2
    else:
        try:
            avg_price = int(price_str.replace('$', ''))
        except ValueError:
            print("無效價格")
            return -1
            
    # 根據平均價格做分類
    if avg_price <= 300:
        return 0
    elif 301 <= avg_price <= 999:
        return 1
    else:
        return 2

# 測試範例
# test_prices = ["$", "$$", "$$$", "1000-1400", "200-400", "超過2000", "250", "800", "1200"]
# for price in test_prices:
#     print(f"{price}: {categorize_price(price)}")

def restaurant_name_to_ai(data):
    # 擷取正確定餐廳名稱 沒在使用的時候，ai都要設false
    restaurant_name_source = data.get('RestaurantName')
    restaurant_type_source = data.get('RestaurantType') 

    restaurant_name = restaurant_name_source
    restaurant_type = restaurant_type_source
    restaurant_main_type = ""
    restaurant_middle_type = ""

    # 有餐廳名稱才跑ai
    if  restaurant_name:
         
        restaurant_name_correct = execute_ai_model(restaurant_name_source, "get_name", False)

        if  restaurant_name_correct:
            restaurant_name = restaurant_name_correct

        # 取得明確的餐廳類別，沒有餐廳類別或餐廳類別不明確才跑類別AI
        if not restaurant_type_source or restaurant_type_source =="餐廳":
            restaurant_type_correct = execute_ai_model(restaurant_name_source, "get_type", False)

            if  restaurant_type_correct:
                restaurant_type = restaurant_type_correct

        # 取得餐廳大類別及中類，新增一個欄位
        restaurant_main_middle_type = execute_ai_model(restaurant_name, "get_main_type", False)

        if restaurant_main_middle_type:
            types = restaurant_main_middle_type.split("-", maxsplit=1)
            if len(types) == 2:
                restaurant_main_type = types[0].strip()
                restaurant_middle_type = types[1].strip()
                print(f"大類: {restaurant_main_type}, 中類: {restaurant_middle_type}")

    return restaurant_name, restaurant_type, restaurant_main_type, restaurant_middle_type 

# 儲存餐廳資訊到mysql
def insert_restaurant_info_to_mysql(data, processed_phone_numbers):
    
    phone_number = data.get('PhoneNumber', '')

    #如果沒有電話號碼，此筆資料跳過
    if not phone_number:
        return processed_phone_numbers, None
    phone_number = phone_number.replace(' ', '')

    # 如果電話號碼已經處理過，則跳過該筆資料
    if phone_number in processed_phone_numbers:
        return processed_phone_numbers, None
    
    # 取得縣市與行政區
    address = data.get('Address', '')
    postal_code, city, district = extract_address(address)

    if len(city) > 3 : 
        city = ""
        
    if len(district) > 3:
        district = ""

    # 將電話號碼加入已處理集合
    processed_phone_numbers.add(phone_number)

    # comment_count: # 移除 CommentCount 中的逗號並轉換為整數
    comment_count = data.get('CommentCount', '').replace(',', '')
    try:
        comment_count = int(comment_count)
    except ValueError:
        comment_count = 0  # 如果無法轉換，則設為 0

    # 取得價格等級
    price_level = get_price_level(data.get('PriceRange'))

   # 取得明確的餐廳名稱及類別，尚未將大類insert資料表
    short_restaurant_name, restaurant_type, restaurant_main_type, restaurant_middle_type = restaurant_name_to_ai(data)    

    restaurant_info = {
        "Restaurant_ID": str(uuid.uuid4()),
        "Restaurant_Name": data.get('RestaurantName'),
        "Short_Restaurant_Name": short_restaurant_name[:500] if short_restaurant_name else "",
        "Restaurant_Type": restaurant_type,
        "Restaurant_Main_Type": restaurant_main_type[:20] if restaurant_main_type else "",
        "Restaurant_Middle_Type": restaurant_middle_type[:20] if restaurant_middle_type else "",
        "Rating": data.get('StarPoint'),
        "Total_Comment": comment_count,
        "Postal_Code": postal_code[:6],
        "City": city,
        "District": district,
        "FullAddress": address,
        "Phone_Number": phone_number,
        "CreatedAt": datetime.now(),   
        "ModifiedAt": datetime.now(),
        "Source": "Google",
        "Price_level": price_level
    }
    
    # query for insert
    insert_query = """
    INSERT INTO Google_Restaurants_Info (
    Restaurant_ID, 
    Restaurant_Name, 
    Short_Restaurant_Name, 
    Restaurant_Type, 
    Restaurant_Main_Type, 
    Restaurant_Middle_Type, 
    Rating, 
    Total_Comment, 
    Postal_Code,
    City, 
    District, 
    FullAddress, 
    Phone_Number, 
    CreatedAt, 
    ModifiedAt, 
    Source, 
    Price_Level)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    # 要插入的資料, 這裡要注意資料的順序要跟 query 的順序一致
    insert_data = (
        restaurant_info['Restaurant_ID'],
        restaurant_info['Restaurant_Name'],
        restaurant_info['Short_Restaurant_Name'],
        restaurant_info['Restaurant_Type'],
        restaurant_info['Restaurant_Main_Type'],
        restaurant_info['Restaurant_Middle_Type'],
        restaurant_info['Rating'],
        restaurant_info['Total_Comment'],
        restaurant_info['Postal_Code'],
        restaurant_info['City'],
        restaurant_info['District'],
        restaurant_info['FullAddress'],
        restaurant_info['Phone_Number'],
        restaurant_info['CreatedAt'],
        restaurant_info['ModifiedAt'],
        restaurant_info['Source'],
        restaurant_info['Price_level']
    )
    
    conn = get_connection()

    if conn:
        # 嘗試寫入第一個資料庫
        try:
            cursor = conn.cursor()
            cursor.execute(insert_query, insert_data)
            conn.commit()
            print("餐廳資訊寫入mySql_local資料庫成功")
        except Exception as e:
            print(f"餐廳資訊寫入mySql_local資料庫失敗: {e}")
            conn.rollback()
        finally:
            cursor.close()
            conn.close()


    print("餐廳資訊清洗完畢")
    # print(restaurant_info)
    return processed_phone_numbers, restaurant_info

#儲存餐廳簡介到mysql
def insert_restaurant_intro_to_mysql(data, restaurant_ID):
    df_list = []
    insert_query = """
    INSERT INTO Google_Short_Intro (Restaurant_ID, Title, Details, CreatedAt, ModifiedAt)
    VALUES (%s, %s, %s, %s, %s)
    """

    short_intro = data.get('Intro', [])
    
    # Convert string to Python list
    short_intro_data = json.loads(short_intro)
   
    conn = get_connection()
    cursor = conn.cursor()
    for item in short_intro_data:
        title = item["title"]
        details = ', '.join(item["details"])

        insert_data = (
            restaurant_ID,
            title,
            details,
            datetime.now(),
            datetime.now()
        )

        # 餐廳簡介資料to list
        short_intro_data = {
            "Restaurant_ID": restaurant_ID,
            "Title": item["title"],
            "Details":",".join(item["details"]),
            "CreatedAt": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "ModifiedAt":datetime.now().strftime("%Y-%m-%d %H:%M:%S") 
        }
        df_list.append(short_intro_data)

        if conn:
            try:
                
                cursor.execute(insert_query, (insert_data))
                print("簡介資料已儲存至 MySQL")
            except Exception as e:
                print(f"簡介資料儲存至 MySQL失敗: {e}")
                continue  # Skip this intro if error occurs
            
    conn.commit()
    cursor.close()
    conn.close()
    print("簡介資料已清洗完畢")

    return df_list

# 儲存營業時間至mysql
def insert_restaurant_open_time_to_mysql(data, restaurant_ID):

    data_to_df_list=[]
    # open_time_source = ["星期六、15:00 到 00:00、17:00 到 22:00; 星期日、15:00 到 00:00; 星期一、15:00 到 00:00; 星期二、休息; 星期三、15:00 到 00:00; 星期四、休息; 星期五、15:00 到 00:00. 隱藏本週營業時間"]
    # open_time_source = None
    # restaurant_ID = str(uuid.uuid4())
    # 以上為測試資料，可刪除

    #同餐廳每天的營業時間
    open_time_info_list = []

    # mongodb的資料
    serve_Time = data.get('ServeTime', "")
    open_time_source = json.loads(serve_Time)

    #如果沒有抓到資料，只存restaurant_id, close_time null, open_time null, weekday null status -1
    if open_time_source:
        day_of_week = {
            "星期一" : 0, 
            "星期二" : 1, 
            "星期三" : 2, 
            "星期四" : 3, 
            "星期五" : 4, 
            "星期六" : 5,
            "星期日" : 6
        }

        open_time_str = open_time_source[0].replace("隱藏本週營業時間", "").strip()
        days_data = open_time_str.split(";")

         # 相同的餐廳資訊，同天同Weekday
        for day_data in days_data:
           
            match = re.match(r"(星期[一二三四五六日])(?:、(休息|(?:[0-9:]+ 到 [0-9:]+)(?:、[0-9:]+ 到 [0-9:]+)*))?", day_data.strip())
            if match:
                time_info = match.group(2)  # 營業時間或休息
                
                #如果是休息僅會有一筆資料
                if time_info == "休息":
                    
                    open_time_info = Open_Time_Info()
                    open_time_info.weekday= day_of_week[match.group(1)]  # 星期
                    
                    open_time_info.restaurant_id = restaurant_ID
                    open_time_info.created_at = datetime.now()
                    open_time_info.modified_at = datetime.now()
                    open_time_info.opening_status = 0# 設置營業狀態為休息
                    open_time_info.open_time = None
                    open_time_info.close_time = None

                    open_time_info_list.append(open_time_info)

                else:
                    time_ranges = re.split(r"[、]", time_info.strip(".")) if time_info else []
                    for time_range in time_ranges:
                        open_time_info = Open_Time_Info()

                        open_time_info.weekday= day_of_week[match.group(1)]  # 星期
                        open_time_info.restaurant_id = restaurant_ID
                        open_time_info.created_at = datetime.now()
                        open_time_info.modified_at = datetime.now()

                        open_time, close_time = time_range.split(" 到 ")
                        print(f"{open_time_info.weekday}: {open_time} - {close_time}")
                        open_time_info.opening_status = 1  # 設置營業狀態為營業中
                        open_time_info.open_time = datetime.strptime(open_time, "%H:%M").time()
                        open_time_info.close_time = datetime.strptime(close_time, "%H:%M").time()
                        # print(f"{open_time_info.weekday}: {open_time_info.open_time} - {open_time_info.close_time}")
                        open_time_info_list.append(open_time_info)          

    else:
        # 同一間餐廳會有相同的retaurant_ID
        open_time_info = Open_Time_Info()
        open_time_info.restaurant_id = restaurant_ID
        open_time_info.created_at = datetime.now()
        open_time_info.modified_at = datetime.now()
        open_time_info.opening_status = -1
        open_time_info_list.append(open_time_info)
        # print(f"沒有抓到資料，只存restaurant_id: {restaurant_ID}, close_time null, open_time null, weekday null status -1")
    
    # 插入資料的 SQL 查詢
    insert_query = """
                    INSERT INTO Google_Opening_Hours (Restaurant_ID, Weekday, Open_time, Close_time, CreatedAt, ModifiedAt, Opening_Status) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """

    data_to_mysql = []
    for opening_info in open_time_info_list:
        data_to_mysql.append(
            (
                opening_info.restaurant_id,  
                opening_info.weekday,       
                opening_info.open_time,
                opening_info.close_time,
                opening_info.created_at,
                opening_info.modified_at,
                opening_info.opening_status
            )
        )

        # 餐廳營業時間資料to list
        open_time_data = {
            "Restaurant_ID": opening_info.restaurant_id,
            "Weekday": opening_info.weekday,
            "Open_time": opening_info.open_time,
            "Close_time": opening_info.close_time,
            "CreatedAt": opening_info.created_at.strftime("%Y-%m-%d %H:%M:%S"),
            "ModifiedAt": opening_info.modified_at.strftime("%Y-%m-%d %H:%M:%S"),
            "Opening_Status": opening_info.opening_status
        }
        data_to_df_list.append(open_time_data)

    # 存資料進local mysql
    conn = get_connection()
    if conn:
        try:
            with conn.cursor() as cursor:
                cursor.executemany(insert_query, data_to_mysql)
                conn.commit()
                print(f"營業時間存入local mysql成功")
        except Exception as e:
            print(f"營業時間存入local mysql失敗: {e}")
        finally:
            conn.close()


    print("營業時間資料已清洗完畢")
    # for info in open_time_info_list:
    #     print(info.restaurant_id, info.weekday, info.open_time, info.close_time, info.created_at, info.modified_at, info.opening_status)
                    
    return data_to_df_list

# 轉換評論時間，這裡的crawler_date是爬蟲的日期，用來計算相對時間
def change_comment_date(comment_time, crawler_date):
    try:
        if not comment_time:
            return None
        
        match = re.match(r"(\d+)\s*(天|週|個?月|年)前", comment_time.replace(" ", ""))

        if not match:
            return None  # 無法解析則回傳 None
    
        num = int(match.group(1))
        unit = match.group(2)

        #確認時間單位
        match unit:
            case "個月" | "月":
                delta = relativedelta(months = num)
        
            case "年":
                delta = relativedelta(years = num)
        
            case "週":
                delta = relativedelta(weeks = num)
    
            case "天":
                delta = relativedelta(days = num)

            case _:
                return None
        comment_date = crawler_date - delta
        return comment_date
    
    except Exception as e:
            print(f"錯誤: {e}")
            return None  # 發生錯誤時返回 None

# 清空mysql餐廳相關資料
def delete_restaurants_data(delete_tables):
    is_deleted = False

    try:
        conn = get_connection()
        cursor = conn.cursor()
        deleted_counts = {}

        # 依序刪除每個表格
        for table in delete_tables:
            cursor.execute(f"DELETE FROM {table}")
            deleted_counts[table] = cursor.rowcount  # 紀錄刪除筆數

        deleted_rows = cursor.rowcount  # 獲取刪除的筆數(有可能一筆都沒有就不用刪除)

        # 確保所有表格資料已清空
        for table in delete_tables:
            cursor.execute(f"SELECT COUNT(*) AS count FROM {table}")
            remaining_rows = cursor.fetchone()[0]
            
            if remaining_rows > 0:
                conn.rollback()  # 如果有表沒刪乾淨，則取消所有操作
                print(f"刪除 {table} 失敗，回滾所有操作")
                return is_deleted
                    
        # 確保資料真的刪除了，才 commit
        conn.commit()

        # 如果所有表都刪除成功，則設為 True
        is_deleted = True
        print("舊資料刪除成功")
        
    except Exception as e:
        conn.rollback()
        print("發生錯誤：", e)
        is_deleted = False

    finally:
        cursor.close()
        conn.close()
        return is_deleted

#整理好的評論資料儲存至Mongo
def insert_comment_to_mongo(source_data, restaurant_ID):

    # mongodb的資料
    comments_monogo_source = source_data.get('Comments', [])
    comments_source = json.loads(comments_monogo_source)
    
    #沒有comment的資料會直接return
    if not comments_source:
        return 
    
    # 整理comment_info物件
    comment_info_model_list = []
    for comment in comments_source:
        comment_info_model = comment_info()
        comment_info_model.commenter_name = comment["commenter_name"]
        comment_info_model.commenter_detail = comment["commenter_detail"]
        comment_info_model.comment_star_point = comment["comment_star_point"]
        comment_info_model.comment_date_record = comment["comment_time"] #原始評論時間

        crawler_date_str = comment["comment_crawler_date"] 
        crawler_date = datetime.strptime(crawler_date_str, "%Y-%m-%d %H:%M:%S")
        # crawler_date = datetime.now() #測試用
        comment_info_model.comment_date = change_comment_date(comment["comment_time"], crawler_date)
        comment_info_model.comment_content = comment["comment_content"]
        comment_info_model_list.append(comment_info_model.to_dict())
    
    comment_data = {
        "_id": restaurant_ID,
        "comments_Info": comment_info_model_list,
        "created_at": datetime.now()
    }

    try:
        # 連接資料庫
        client, db = get_mongo_connection()

        collection = db["google_restaurants_comments"]     

        # 新增資料
        collection.insert_one(comment_data)

        # 關閉連線
        # client.close()

        print("評論資料資料儲存至mongodb完成")
    except Exception as e:
        print(f"Error occurred: {e}")
    finally:
        client.close()

def delete_comments_data():

    is_deleted = False 

    try:
        client, db = get_mongo_connection()
        collection = db["google_restaurants_comments"]

        # 刪除所有資料
        collection.delete_many({})

        print("刪除評論資料成功")
        is_deleted = True
        
    except Exception as e:
        print(f"刪除評論資料異常: {e}")
        is_deleted = False

    finally:
        client.close()
        return is_deleted
    
def main():
    try:
        # 從mongoDB取得資料
        data_source = get_source_data() 

        if not data_source:
            print("沒有資料需要更新")
            return
        
        # 已處理過的電話號碼
        processed_phone_numbers = set()

        restaurant_info_list = []
        short_intro_data_list= []
        opening_hours_list = []

        delete_tables = ["Google_Restaurants_Info", "Google_Short_Intro", "Google_Opening_Hours"]

        # 有資料應該先清空資料庫
        is_deleted = delete_restaurants_data(delete_tables)

        if not is_deleted:
            print("餐廳相關資料刪除失敗，停止新增資料")
            return
        
        is_comments_deleted = delete_comments_data()

        if not is_comments_deleted:
            print("評論資料刪除失敗，停止新增資料")
            return
        
        for idx, data in enumerate(data_source):
            # if idx >= 5: # Test
            #     print("----->>>>> 測試執行結束")
            #     break
            
            # 將餐廳基本資料插入到MySQL
            processed_phone_numbers, restaurant_info = insert_restaurant_info_to_mysql(data, processed_phone_numbers)
            
            if not restaurant_info:
                continue

            restaurant_ID = restaurant_info["Restaurant_ID"]

            restaurant_info_list.append(restaurant_info)

            # 將餐廳簡介資料插入到MySQL
            short_intro_dfs = insert_restaurant_intro_to_mysql(data, restaurant_ID)
            for short_intro_df in short_intro_dfs:
                short_intro_data_list.append(short_intro_df)

            # 將餐廳營業時間插入到MySQL
            opening_hours_dfs = insert_restaurant_open_time_to_mysql(data, restaurant_ID)
            for opening_hours_df in opening_hours_dfs:
                opening_hours_list.append(opening_hours_df)

            # 將餐廳評論資料插入到mongoDB，_id 存mysql的restaurant_ID
            insert_comment_to_mongo(data, restaurant_ID)
        
        # 存入csv就順便存入gcs
        # 餐廳資料進csv excel
        ri_df = pd.DataFrame(restaurant_info_list)

        save_dataframe(ri_df, filename_prefix="Google_Restaurants_Info",folder="cleaning_output", type="cleaning")

        # 餐廳簡介資料進csv excel
        si_df = pd.DataFrame(short_intro_data_list)
        save_dataframe(si_df, filename_prefix="Google_Short_Intro", folder="cleaning_output", type="cleaning")

        # 餐廳營業時間資料進csv excel
        oh_df = pd.DataFrame(opening_hours_list)

        #回傳檔名
        save_dataframe(oh_df, filename_prefix="Google_Opening_Hours", folder="cleaning_output", type="cleaning")

    except Exception as e:
        print(f"Error occurred: {e}")
        return

if __name__ == "__main__":
    main()