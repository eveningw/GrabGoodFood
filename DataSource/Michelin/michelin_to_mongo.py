##### 這個檔案是爬取米其林餐廳的資料, 並存入MongoDB #####
# 1. 先清空michelin collection
# 2. 爬取資料
# 3. 將資料存入michelin collection

import requests
from bs4 import BeautifulSoup
import time
from pymongo import MongoClient
import re

Headers = {"user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.93 Safari/537.36"}
# 爬首頁, 取出第page_num頁的HTML存入soup回傳, soup會回傳每一頁的HTML
# 傳入頁碼, 取得餐廳url, Request 取得HTML 存入soup
def Fetch_Page_Content(page_num: int) -> BeautifulSoup :
    Url = f"https://guide.michelin.com/tw/zh_TW/taipei-region/taipei/restaurants/page/{page_num}?sort=distance"        

    response = requests.get(Url, headers=Headers)
    
    if response.status_code != 200:
        return None
    
    soup = BeautifulSoup(response.text, "html.parser")
    response.close() # 手動關閉連線
    return soup

# 抓取網頁上的餐廳資訊(電話、地址、餐廳介紹、餐廳類別、網頁頁數), 並存入 miche_restuants.csv
def Scrape_Michelin_Resturants():  
    # 紀錄當下日期時間
    Fetch_Dt = time.strftime( '%Y-%m-%d %H:%M:%S', time.localtime())    
    All_Resturants = []
    name_set=set()

    # 每次執行前先清空michelin collection
    # 連接到 MongoDB
    client = MongoClient("localhost", 27017)
    db = client["proj_db"]
    colName = "michelin"

    # 刪除舊的 collection
    if colName in db.list_collection_names():
        db[colName].drop()
        print(f"The collection '{colName}' has been dropped.")

    # 爬取資料
    page = 1
    while True:
        # 取得米其林網站每個page的HTML存入soup
        soup = Fetch_Page_Content(page)

        if not soup or not soup.select('h3 a'):
            break

        cnt = 1       
        # 由page的HTML soup逐一找出'h3 a['href'] => /tw/zh_TW/taipei-region/taipei/restaurant/kiku-1216129 存到Resturant_Url 再傳入 Fetch_Resturant_Data
        for a in soup.select('h3 a'):          
            Resturant_Url = f"https://guide.michelin.com{a['href']}"  # https://guide.michelin.com/tw/zh_TW/taipei-region/taipei/restaurant/kiku-1216129 掬
            resturant_data = Fetch_Resturant_Data(Resturant_Url, Headers, Fetch_Dt ) 

            if resturant_data["Restaurant_Name"] not in name_set:
                name_set.add(resturant_data["Restaurant_Name"])
                All_Resturants.append(resturant_data)            
                cnt += 1
        page += 1

    if All_Resturants:
        # 因為前面已經清空了michelin collection, 所以這裡不用再檢查collection是否存在
        # client = MongoClient("localhost", 27017)
        # db = client["proj_db"]
        # colName =  "michelin"

        # 檢查collection是否存在
        if colName not in db.list_collection_names():
            print(f"The collection '{colName}' does not exist. Creating new collection.")
            db.create_collection(colName)
        col= db[colName]
        col.insert_many(All_Resturants) 

# 把各餐廳的HTML抓回來存入 detail_soup, 再到 detail_soup , 將每間餐廳的資訊逐一取出組成 dictionary 回傳
# def Fetch_Resturant_Data( Page_info:str, url:str, headers:str, Fetch_Dt:str ) -> dict:
def Fetch_Resturant_Data( url:str, headers:str, Fetch_Dt:str ) -> dict:
    response = requests.get(url, headers=headers)
    detail_soup = BeautifulSoup(response.text, "html.parser")
    response.close()

    # +886 2 2720 6417
    Phone_Number = detail_soup.select_one("a.phone")

    # 都一處 (信義)
    Restaurant_Name = detail_soup.select_one("h1")

    # 信義區仁愛路四段506號, Taipei, 110, 臺灣
    address = detail_soup.select_one("div.data-sheet__block--text")

    # 評論
    Comment_Content = detail_soup.select_one("div.data-sheet__description")

    # $$·京菜
    type = detail_soup.select_one("div.data-sheet__block--text:nth-of-type(2)")

    if "Taipei" in address.text:
        City = "Taipei"
    elif "Tainan" in address.text:
        City = "Tainan"
    else:
        City = None    
    
    # 找出地址中的xx區
    district_match = re.search(r'(.{2}區)', address.text)

    # 取得第一個符合的xx區字串
    District = district_match.group(1) if district_match else None 

    return {
                "Phone_Number": Phone_Number.text.strip() if Phone_Number else None,
                "Restaurant_Name" : Restaurant_Name.text.strip() if Restaurant_Name else None,
                "FullAddress" : address.text.strip() if address else None,
                "District": District,
                "City": City ,
                "Comment_Content" : Comment_Content.text.strip() if Comment_Content else None,
                "type" : type.text.replace(" ","").replace("\n","") if type else None,
                "CreatedAt": Fetch_Dt,
                "ModifiedAt": None
            }

Scrape_Michelin_Resturants()