from datetime import datetime
import json
import logging
from typing import Iterable, List
import pymongo
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By

from bs4 import BeautifulSoup
import time
import re
import csv

from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import asyncio
import pandas as pd
import os

from webdriver_manager.chrome import ChromeDriverManager
from concurrent.futures import ThreadPoolExecutor
from pymongo import MongoClient

from datetime import datetime, timedelta
import re

from Utils.gcsHelper import upload_to_gcs
from Utils.mongoDBHelper import get_mongo_connection
from Utils.saveDataFrameHelper import save_dataframe

MAX_SCROLL = None
MAX_GRAB = None

LOG_FOLDER = './Log'

os.makedirs(LOG_FOLDER, exist_ok=True)
log_filename = datetime.now().strftime("%Y-%m-%d") + ".txt"
log_fmt = "%(asctime)s [%(levelname)s] - %(message)s"
logging.basicConfig(
    level=logging.INFO,
    format=log_fmt,
    encoding='utf-8',
    handlers=[
        logging.FileHandler(f"{LOG_FOLDER}/{log_filename}"),
        logging.StreamHandler()  # To also display logs in the console
    ]
)


class RestaurantInfo:
    def __init__(self):
        self.url = ""
        self.title = ""
        self.restaurant_type = ""
        self.starPoint = "" 
        self.price_range = ""
        self.open_time_list = []
        self.commentTotal = ""
        self.address = ""
        self.phone_number = ""
        self.intro_list = []
        self.all_comments = []
        

# 初始化 Selenium 驅動
def initialize_driver():
    # service = Service(executable_path="./chromedriver")
    # 自動安裝對應 Chrome 版本的 ChromeDriver
    service = Service(ChromeDriverManager().install())
    options = webdriver.ChromeOptions()
    driver = webdriver.Chrome(service=service, options=options)
    #driver = webdriver.Chrome()
    #driver.maximize_window()
    return driver

# 使用元素滾動，並且獲取完整 HTML (前一版)
def page_down_in_comments(driver, url):
    driver.get(url)
    divSideBar=driver.find_element(By.CLASS_NAME,"m6QErb.DxyBCb.kA9KIf.dS8AEf.XiKgde")

    keepScrolling=True
    scroll_count = 0

    pre_total_comment = -1
    
    

    while(keepScrolling):
        if MAX_SCROLL != None and scroll_count >= MAX_SCROLL:
            break

        for _ in range(4):
            divSideBar.send_keys(Keys.PAGE_DOWN)
            time.sleep(0.5)

        more_details_buttons = driver.find_elements(By.CLASS_NAME, "w8nwRe.kyuRq")
        # time_elements = driver.find_elements(By.CLASS_NAME, "rsqaWe")

        for button in more_details_buttons:
            if button.text == "全文":
                button = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.CLASS_NAME, "w8nwRe.kyuRq")))
                button.click()
                # WebDriverWait(driver, 3).until(EC.staleness_of(button))  # 等待按鈕消失
                time.sleep(2) #改成1試試

        all_comments_div = driver.find_elements(By.CLASS_NAME, "jftiEf.fontBodyMedium")
        cur_total_comment = len(all_comments_div)
        # print(f"滾動後評論數: {cur_total_comment}")

        if cur_total_comment == pre_total_comment:
            no_change_count += 1
            if no_change_count >= 3: # 連續三次高度沒變化，判定為到底
                keepScrolling = False
                #print("滾動後數量沒有變化，停止滾動。")
            # else:
            #     print(f"數量沒有變化，但是還沒有連續三次，繼續滾動。(次數: {no_change_count})")
        else:
            pre_total_comment = cur_total_comment
            no_change_count = 0

        scroll_count += 1

    return driver.page_source


# 定義函數來清理不可見字符和換行符
def clean_characters(text):
    """
    移除不可見的 Unicode 特殊字符和換行符，但保留其他可見內容。
    """
    # 使用正則表達式移除私有區的不可見字符（\ue000 至 \uf8ff）以及換行符 \n
    cleaned_text = re.sub(r'[\U00010000-\U0010FFFF\uE000-\uF8FF\n]+', '', text)
    return cleaned_text


# 滾動後取得該此搜尋所有餐廳完整url
def scroll_all_restaurant(driver, url, search_query):
    driver.get(url)
    divSideBar=driver.find_element(By.CSS_SELECTOR,f"div[aria-label='「{search_query}」的搜尋結果']")

    keepScrolling=True
    scroll_count = 0
    while(keepScrolling):
        if MAX_SCROLL != None and scroll_count >= MAX_SCROLL:
            break

        for _ in range(2):
            divSideBar.send_keys(Keys.PAGE_DOWN)
            time.sleep(0.5)

        # divSideBar.send_keys(Keys.PAGE_DOWN)
        # time.sleep(0.5)
        # divSideBar.send_keys(Keys.PAGE_DOWN)
        # time.sleep(0.5)
        html=driver.find_element(By.TAG_NAME, "html").get_attribute('outerHTML')

        if html.find("你已看完所有搜尋結果。") != -1:
            keepScrolling=False

        scroll_count += 1

    return driver.page_source

#取得營業時間
def getPlaceTimeData(soup, restaurant_title):
    try:
        open_time_list = []

        time_div = soup.select_one("div.t39EBf.GUrTXd")
        if time_div:
            # 星期五 (新春 (補假)/春節 (補假))、11:30 到 15:00、17:30 到 22:30、營業時間可能不同; 星期六、11:30 到 15:00、17:30 到 22:30; 星期日、11:30 到 15:00、17:30 到 22:30; 星期一、11:30 到 15:00、17:30 到 22:30; 星期二、11:30 到 15:00、17:30 到 22:30; 星期三、11:30 到 15:00、17:30 到 22:30; 星期四、11:30 到 15:00、17:30 到 22:30. 隱藏本週營業時間
            time_data = time_div.get("aria-label", "")
            open_time_list.append(clean_characters(time_data) if time_data else "")
        else:
            open_time_list = []
            print( f"{restaurant_title} 無法取得營業時間")

        return open_time_list
    except Exception as e:
        print( f"{restaurant_title} 取得營業時間異常 錯誤資訊: {e}")
        logging.info(f"{datetime.now()} Error! {restaurant_title} - {(str)(e)}")

#判斷是否為最近的評論, 這個函數沒有使用
# def is_recent_comment(comment_time_text):
#     match = re.search(r'(\d+) 年前', comment_time_text)
#     if match:
#         years_ago = int(match.group(1))
#         return years_ago <= 1  # 只保留 1 年內的資料
#     return True  # 如果沒有 "X 年前"，則視為最近的資料


# 總覽
def get_restaurant_summary(driver, restaurant : RestaurantInfo):
    html = driver.page_source
    soup = BeautifulSoup(html, "html.parser")
    
    restaurant_type = soup.select_one("div.skqShb .DkEaL")
    restaurant.restaurant_type = restaurant_type.text.strip() if restaurant_type else ""

    # address = soup.select_one("div.Io6YTe.fontBodyMedium")
    # restaurant.address = address.text.strip() if address else "" 
    restaurant.open_time_list = getPlaceTimeData(soup, restaurant.title)

    #取得電話號碼
    info_div_list = soup.select("div.RcCsl.fVHpi.w4vB1d.NOE9ve.M0S7ae.AG25L")
    for info_div in info_div_list:
        btn_aria_label = info_div.select_one('button[aria-label]')
        aria_label_value = btn_aria_label.get("aria-label", "") if btn_aria_label else ""
        
        if aria_label_value.find("地址") != -1:
            restaurant.address = aria_label_value.split(":")[1].strip()

        if aria_label_value.find("電話號碼") != -1:
            restaurant.phone_number = aria_label_value.split(":")[1].strip()


    info_elements = WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div.Io6YTe.fontBodyMedium.kR99db.fdkmkc")))
    print("評論區的餐廳資訊元素數量:", len(info_elements))  # 檢查找到幾個元素
    time.sleep(1)


    #換到評論區
    top_buttons = driver.find_elements(By.CLASS_NAME, "hh2c6")
    top_buttons[1].click()
    # 正確做法 https://stackoverflow.com/questions/77659569/reviews-button-is-not-clicking-on-google-map
    # 等待元素可见
    time.sleep(2) # 先wait解問題

    driver.get(driver.current_url)
    html = driver.page_source
    
    # 評論相關按鈕，選擇排序按鈕
    comment_buttons = driver.find_elements(By.CLASS_NAME, "g88MCb.S9kvJb")

    for button in comment_buttons:
        if button.get_attribute("aria-label") == "排序評論":
            button.click()
            time.sleep(2)
            break
    
    # 選擇評論類型
    data_index = 1 # 最新

    # 找到排序方法的按鈕
    category_click = driver.find_elements(By.CLASS_NAME, 'fxNQSd')
    print(category_click[data_index].text)

    # 點擊需要的排序方法
    category_click[data_index].click()
    time.sleep(2)

    # 呼叫評論區的pagedown
    html = page_down_in_comments(driver, driver.current_url)
    soup = BeautifulSoup(html, "html.parser")
    
    all_comments_div = soup.select("div.jftiEf.fontBodyMedium")
    
    # all_comments_div = driver.find_elements(By.CLASS_NAME, "jftiEf.fontBodyMedium")
    print(f"{restaurant.title} 取得評論數: {len(all_comments_div)}")

    
    for one_comment in all_comments_div:
        commenter_Info_div = one_comment.select_one("div.WNxzHc.qLhwHc")
        commenter_name = commenter_Info_div.select_one("div.d4r55") 
        commenter_name_text = commenter_name.text.strip() if commenter_name else ""

        commenter_detail = commenter_Info_div.select_one("div.RfnDt")
        commenter_detail_text = commenter_detail.text.strip() if commenter_detail else ""

        #評星及時間
        comment_star_point_area = one_comment.select_one("div.GHT2ce .DU9Pgb")

        comment_star_point_label = comment_star_point_area.select_one('span[aria-label]')
        comment_star_point_value = comment_star_point_label.get("aria-label", "") if comment_star_point_label else ""

        comment_time = comment_star_point_area.select_one("span.rsqaWe")
        comment_time_text = comment_time.text.strip() if comment_time else ""

        comment_content = one_comment.select_one("div.MyEned")
        comment_content_text = clean_characters(comment_content.text.strip()) if comment_content else ""

        # comment_text = clean_characters(one_comment.get_attribute('innerText')) if one_comment else ""

        
        
        restaurant.all_comments.append({
            "commenter_name": commenter_name_text,
            "commenter_detail": commenter_detail_text,
            "comment_star_point": comment_star_point_value,
            "comment_time": comment_time_text,
            "comment_content": comment_content_text,
            "comment_crawler_date" : datetime.now().strftime("%Y-%m-%d %H:%M:%S") #新增一個該評論抓到的日期
        })


    
    print(f"{restaurant.title} 開始抓簡介區")
    # 換到簡介區
    
    top_buttons = driver.find_elements(By.CLASS_NAME, "hh2c6")
    top_buttons[2].click()
    time.sleep(2)

    #取得html
    driver.get(driver.current_url)
    intro_html = driver.page_source
    intro_soup = BeautifulSoup(intro_html, "html.parser")
    intro_list_Wrapper = intro_soup.select_one("div.m6QErb.DxyBCb.kA9KIf.dS8AEf.XiKgde ")
    intro_div_list = intro_list_Wrapper.select("div.iP2t7d.fontBodyMedium")
    for intro_div in intro_div_list:
        title = intro_div.select_one("h2.iL3Qke.fontTitleSmall")
        title_text = title.text.strip() if title else ""
        
        details_list = []
        details_div = intro_div.select_one("ul.ZQ6we")
        details_li_list = details_div.select("li.hpLkke")

        for details_li in details_li_list:
            details_list.append(details_li.text.replace("\ue5ca", "有").replace("\ue033", "沒有").strip() if details_li else "")
        
        restaurant.intro_list.append({
            "title": title_text,
            "details": details_list
        })

    print(f"{restaurant.title} 簡介區抓取完畢")
    print(f"summary:\n餐廳:{restaurant.title} \n 餐廳類別: {restaurant.restaurant_type}\n 評星: {restaurant.starPoint}\n 評論數:{restaurant.commentTotal} \n價錢: {restaurant.price_range}\n 地址:{restaurant.address}\n 營業時間:{restaurant.open_time_list} \n 電話號碼: {restaurant.phone_number}\n 簡介區: {restaurant.intro_list} \n 評論區:{len(restaurant.all_comments)}\n url: {restaurant.url}")




def get_all_restaurants_info(driver, restaurant_part_Infos):
    completed_Info_list = []
    for part_info in restaurant_part_Infos:
        
        # 應該在這裡整理資料
        driver.get(part_info.url)
        

        completed_Info = get_restaurant_summary(driver, part_info)
        completed_Info_list.append(completed_Info)

        # 匯出資料到 CSV 檔案
    with open('restaurant_info.csv', mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(['餐廳名稱', '餐廳類別', '評星', '評論數', '價格範圍', '地址', '營業時間', '電話號碼', '簡介區', '評論區', '抓取時間'])

        # 寫入所有餐廳資訊
        for completed_Info in completed_Info_list:
            writer.writerow([
                completed_Info.title,
                completed_Info.restaurant_type,
                completed_Info.starPoint,
                completed_Info.commentTotal,
                completed_Info.price_range,
                completed_Info.address,
                json.dumps(completed_Info.open_time_list, ensure_ascii=False),
                completed_Info.phone_number,
                json.dumps(completed_Info.intro_list, ensure_ascii=False),
                json.dumps(completed_Info.all_comments, ensure_ascii=False),
                datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            ])


# 異步取得所有餐廳資訊
def get_all_restaurants_info_async(restaurantInfos_chunk : List[RestaurantInfo]):
    chunk_size = len(restaurantInfos_chunk)
    for idx, restaurant in enumerate(restaurantInfos_chunk):
        start_grab_time = time.time()
        print(f"--- Start new grab: {restaurant.title} - {idx + 1}/{chunk_size}")
        # 建立一個新的瀏覽器驅動
        driver = initialize_driver()
        try:
            driver.get(restaurant.url)

            # 取得餐廳資訊
            get_restaurant_summary(driver, restaurant)
        except Exception as e:
            print(f"get_all_restaurants_info_async {restaurant.title} Error occurred: {e}")
        finally:
            # 關閉瀏覽器驅動
            driver.quit()
            end_grab_time = time.time()
            print(f"--- End grab: {restaurant.title} - Spend time: {end_grab_time - start_grab_time} (s)")
    

# 取得所有餐廳網址搜尋結果
def get_all_restaurants(url, search_query) -> List[RestaurantInfo]:
    driver = initialize_driver()

    try:
        # 使用元素滾動並獲取完整 HTML
        html = scroll_all_restaurant(driver, url, search_query)

        soup = BeautifulSoup(html, "html.parser")

        grab_count = 0
        restaurantInfos = []

        for article in soup.select("div.Nv2PK.THOPZb.CpccDe"):
            if MAX_GRAB != None and grab_count >= MAX_GRAB:
                break

            restaurantInfo = RestaurantInfo()

            title = article.select_one(".qBF1Pd.fontHeadlineSmall")
            restaurantInfo.title = title.text.strip() if title else ""

            starPoint = article.select_one(".MW4etd")
            restaurantInfo.starPoint = starPoint.text.strip() if starPoint else ""

            commentTotal = article.select_one(".UY7F9")
            restaurantInfo.commentTotal = commentTotal.text.strip("()") if commentTotal else ""

            price_range = article.select_one('div.W4Efsd > div.AJB7ye > span:last-of-type > span:last-of-type')

            restaurantInfo.price_range = price_range.text.strip() if price_range and '$' in price_range.text else ""

            href = article.select_one("a.hfpxzc")
            restaurantInfo.url = href.get("href", "") if href else ""

            restaurantInfos.append(restaurantInfo)

            grab_count += 1

    except Exception as e:
        restaurantInfos = None
        print(f"get_all_restaurants Error occurred: {e}")
    finally:
        driver.quit()
       
    return restaurantInfos

def data_to_mongoDB(data):
    
    try:
        if len(data) > 0: #正常來說應該一定會大於600筆

            # 連接資料庫
            # client = pymongo.MongoClient("mongodb://localhost:27017/")
            # db = client["test"]  # 指定資料庫
            client, db = get_mongo_connection()
            collection = db["google_taipei_restaurant_data"] 

            # 新增資料
            collection.insert_many(data)

            print("資料儲存至mongodb完成")
        else:

            print("資料筆數不足，不執行資料新增")

    except Exception as e:
        print(f"Error occurred: {e}")
    finally:
        client.close()
    

def delete_mongo_source_data():

    is_deleted = False 

    try:
        client, db = get_mongo_connection()
        collection = db["google_taipei_restaurant_data"]

        # 刪除所有資料
        collection.delete_many({})

        print("刪除餐廳來源資料成功")
        is_deleted = True
        
    except Exception as e:
        print(f"刪除餐廳來源資料異常: {e}")
        is_deleted = False

    finally:
        client.close()
        return is_deleted

# 主流程
def main(search_keyword_List):

    print(f"開始抓取資料{datetime.now()}")

    # 這裡抓資料之前應該要先刪除mongodb的資料
    for keyword in search_keyword_List:
        start_grab_time = time.time()

        search_query = f"台北{keyword}餐廳"
        url = f"https://www.google.com.tw/maps/search/{search_query}"

        print(f"Start grab {search_query} restaurants.")
        
        # 取得所有餐廳搜尋結果url
        restaurantInfos = get_all_restaurants(url, search_query)
        print(f"Got {len(restaurantInfos)} restaurants.")

        # 將餐廳資訊分成數個chunk，以便異步取得資訊
        splitCount = 10
        chunk_size = (len(restaurantInfos) + splitCount - 1) // splitCount  # Calculate chunk size to ensure splitCount chunks
        restaurant_chunks = [restaurantInfos[i:i + chunk_size] for i in range(0, len(restaurantInfos), chunk_size)]

        # 如果分割後的 chunk 數量大於 splitCount，則將最後一個 chunk 的資料合併到前面的 chunk
        if len(restaurant_chunks) > splitCount:
            restaurant_chunks[splitCount - 1].extend([item for sublist in restaurant_chunks[splitCount:] for item in sublist])
            restaurant_chunks = restaurant_chunks[:splitCount]

        # 異步取得所有餐廳資訊
        loop = asyncio.get_event_loop()
        tasks = [loop.run_in_executor(None, get_all_restaurants_info_async, restaurants) for restaurants in restaurant_chunks]
        loop.run_until_complete(asyncio.wait(tasks))

        # Create a list to store restaurant data
        restaurant_data = []

        # Collect data from restaurantInfos
        for restaurantInfo in restaurantInfos:
            restaurant_data.append({
                'RestaurantName': restaurantInfo.title,
                'RestaurantType': restaurantInfo.restaurant_type,
                'StarPoint': restaurantInfo.starPoint,
                'CommentCount': restaurantInfo.commentTotal,
                'PriceRange': restaurantInfo.price_range,
                'Address': restaurantInfo.address,
                'ServeTime': json.dumps(restaurantInfo.open_time_list, ensure_ascii=False),
                'PhoneNumber': restaurantInfo.phone_number,
                'Intro': json.dumps(restaurantInfo.intro_list, ensure_ascii=False),
                'Comments': json.dumps(restaurantInfo.all_comments, ensure_ascii=False),
                'Url': restaurantInfo.url,
                'CreatedAt': datetime.now()  #新增一個建立日期
            })

        
        # Convert the list to a DataFrame
        df = pd.DataFrame(restaurant_data)

        file_name =f'{keyword}_restaurant'

        # Save the DataFrame to a CSV and Excel file
        save_dataframe(df, file_name, save_csv=True, save_excel=True, folder="source_output", type="source")
        
        end_grab_time = time.time()
        print(f"Total grab spend time: {end_grab_time - start_grab_time} (s)")

        #更新原始資料前，應該要先清空原始資料
        is_deleted = delete_mongo_source_data()

        if is_deleted:
            print("刪除原始資料成功")

            # 將資料存入 MongoDB
            data_to_mongoDB(restaurant_data)
        else:
            print("刪除原始資料失敗，不執行新增來源資料至MongoDB")

if __name__ == "__main__":
        
    #以台北12個行政區分別搜尋以增加餐廳數量
    search_keyword_List = ["北投區", "士林區", "大同區", "中山區", "松山區", "內湖區", "萬華區", "中正區", "大安區", "信義區", "南港區", "文山區"]

    # DEBUG 只滾動N次就好
    debug_mode = True
    if debug_mode:
        search_keyword_List = ["信義區"]
        MAX_SCROLL = 2
        MAX_GRAB = 2
    
    main(search_keyword_List)
   
    
