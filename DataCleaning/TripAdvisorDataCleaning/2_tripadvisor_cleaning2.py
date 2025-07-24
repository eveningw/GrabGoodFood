import pandas as pd
import re
from pymongo import MongoClient
import os
from google.cloud import storage
from google.oauth2.service_account import Credentials
from pathlib import Path

#連接MongoDB
def connect_to_mongo(uri, db_name, collection_name):
    """連接到 MongoDB 並返回資料庫和集合的引用。"""
    try:
        client = MongoClient(uri)
        db = client[db_name]
        collection = db[collection_name]
        return client, db, collection
    except Exception as e:
        print(f"連接 MongoDB 時發生錯誤: {e}")
        return None, None, None

#計算文檔數
def count_documents(collection):
    """返回集合中的文檔總數並打印。"""
    count = collection.count_documents({})
    print("總資料筆數:", count)
    return count

def print_document_fields(collection):
    """獲取一個文檔並打印所有欄位名。"""
    document = collection.find_one()
    
    if document:
        print("所有欄位名:")
        for field in document.keys():
            print(field)
        count = len(document.keys())
        print("總欄位數:", count)
    else:
        print("集合中沒有文檔。")

#指定刪除的欄位
def delete_fields(collection, fields):
    """刪除指定的欄位。"""
    result = collection.update_many(
        {},
        {'$unset': {field: "" for field in fields}}
    )
    print(f"已刪除 {result.modified_count} 筆資料的指定欄位。")

#更新電話格式
def update_phone_format(collection):
    """更新電話格式，將 +886 開頭的電話號碼轉換為 0 開頭的格式。"""
    result = collection.update_many(
    {
        '$or': [
            { 'phone': { '$regex': r'^\+886' } }, # +886開頭
            { 'phone': { '$regex': r'^886' } },  #886開頭
            { 'phone': { '$regex': r'^\+102' } },  #+102 開頭
            { 'phone': { '$regex': r'^102' } },  #102 開頭
            { 'phone': { '$regex': r'^\+\(886\)' } }  #+(886)開頭
        ]
    },
    [
        {
            '$set': {
                'revised_phone': {
                    '$concat': [
                        '0',  # 新的開頭
                        {
                            '$trim': {
                                'input': {
                                    '$replaceAll': {  # 去除所有空格
                                        'input': {
                                            '$replaceAll': {  # 去除所有 -
                                                'input': {
                                                    '$replaceOne': { 
                                                        'input': '$phone',  # 原始電話號碼
                                                        'find': '+886',  # 替換 +886
                                                        'replacement': ''  # 替換為空字符串
                                                    }
                                                },
                                                'find': '-',  # 要替換的字符（-）
                                                'replacement': ''  # 替換為空字符串
                                            }
                                        },
                                        'find': ' ',  # 要替換的字符（空格）
                                        'replacement': ''  # 替換為空字符串
                                    }
                                }
                            }
                        }
                    ]
                }
            }
        }
    ]
)
    print(f"更新了 {result.modified_count} 個文檔。")

# 更新 district 欄位
def update_district_by_postalcode(collection):
    """根據 addressObj.postalcode 的值更新 district 欄位。"""
    result = collection.update_many(
        {
            # 確保 addressObj/postalcode 是字串類型
            'addressObj/postalcode': {'$type': 'string'}
        },
        [
            {
                "$set": {
                    "District": {
                        "$switch": {
                            "branches": [
                                {"case": {"$regexMatch": {"input": "$addressObj/postalcode", "regex": "^100"}}, "then": "中正區"},
                                {"case": {"$regexMatch": {"input": "$addressObj/postalcode", "regex": "^103"}}, "then": "大同區"},
                                {"case": {"$regexMatch": {"input": "$addressObj/postalcode", "regex": "^104"}}, "then": "中山區"},
                                {"case": {"$regexMatch": {"input": "$addressObj/postalcode", "regex": "^105"}}, "then": "松山區"},
                                {"case": {"$regexMatch": {"input": "$addressObj/postalcode", "regex": "^106"}}, "then": "大安區"},
                                {"case": {"$regexMatch": {"input": "$addressObj/postalcode", "regex": "^108"}}, "then": "萬華區"},
                                {"case": {"$regexMatch": {"input": "$addressObj/postalcode", "regex": "^110"}}, "then": "信義區"},
                                {"case": {"$regexMatch": {"input": "$addressObj/postalcode", "regex": "^111"}}, "then": "士林區"},
                                {"case": {"$regexMatch": {"input": "$addressObj/postalcode", "regex": "^112"}}, "then": "北投區"},
                                {"case": {"$regexMatch": {"input": "$addressObj/postalcode", "regex": "^114"}}, "then": "內湖區"},
                                {"case": {"$regexMatch": {"input": "$addressObj/postalcode", "regex": "^115"}}, "then": "南港區"},
                                {"case": {"$regexMatch": {"input": "$addressObj/postalcode", "regex": "^116"}}, "then": "文山區"}
                            ],
                            "default": None  # 如果沒有匹配到，設為 None
                        }
                    }
                }
            }
        ]
    )
    print(f"更新了 {result.modified_count} 筆資料的 district 欄位。")

# 新增欄位重命名函數
def rename_fields_in_collection(collection, rename_map):
    """根據提供的對應字典重命名集合中的欄位。"""
    for old_field, new_field in rename_map.items():
        result = collection.update_many(
            {},
            {'$rename': {old_field: new_field}}
        )
        print(f"欄位 '{old_field}' 已重命名為 '{new_field}'，共更新 {result.modified_count} 筆資料。")
 
def update_district_from_full_address(collection):
    """
    從集合中查詢 District 欄位為空值或 null，且 FullAddress 包含 "區" 的文檔，
    提取區前兩個字+區，並更新回 District 欄位。

    :param collection: MongoDB 集合對象
    :return: 更新的文檔數量
    """
    result = collection.update_many(
        {
            '$or': [
                { 'District': { '$exists': False } },
                { 'District': None },
                { 'District': '' }
            ],
            'FullAddress': { '$regex': '區' }
        },
        [
            {
                '$set': {
                    'District': {
                        '$concat': [
                            {
                                '$substrCP': [
                                    '$FullAddress',
                                    {
                                        '$subtract': [
                                            { '$indexOfCP': ['$FullAddress', '區'] },
                                            2
                                        ]
                                    },
                                    2
                                ]
                            },
                            '區'
                        ]
                    }
                }
            }
        ]
    )
    print(f"更新了 {result.modified_count} 筆資料的 District 欄位。")


# 設定 GCS 認證
GCS_CREDENTIALS_FILE_PATH = "tripadvisor_mongo/artifacts-registry-user.json"
CREDENTIALS = Credentials.from_service_account_file(GCS_CREDENTIALS_FILE_PATH)

# 建立 GCS 客戶端
client = storage.Client(credentials=CREDENTIALS)

# GCS 參數
bucket_name = "tir104-alina"  # 替換為你的 GCS Bucket 名稱


def upload_to_gcs(local_file_path):
    """將本機檔案上傳至 GCS"""
    # 取得檔案名稱
    try:

        filename = os.path.basename(local_file_path)
        blob_name = f"cleaning_output/{filename}" # 替換為要上傳或下載的檔案名稱
        bucket = client.bucket(bucket_name) 
        blob = bucket.blob(blob_name)
        blob.upload_from_filename(local_file_path) # 本機要上傳的檔案
        print(f"檔案 {local_file_path} 已成功上傳至 GCS {bucket_name}/{blob_name}")
    except Exception as e:
        print(f"錯誤: {e}")





def main():
    # 連接到 MongoDB
    # 雲端mongoDB
    # uri = 'mongodb://34.81.181.216:27200'  
    # db_name = 'GrabGoodFood_ETL'
    # collection_name = 'Tripadvisor'
    
    # 地端mongoDB
    uri = 'mongodb://localhost:27017/' 
    db_name = 'tripadvisor'
    collection_name = 'restaurants3'

    client, db, collection = connect_to_mongo(uri, db_name, collection_name)

    # 查看總共有幾筆資料
    # count_documents(collection)

    
    # 刪除欄位示範
    fields_to_delete = [
        'addressObj/country', 'addressObj/state', 'category', 'id', 'image', 'input', 'isClaimedIcon', 'isClaimedText', 
        'isClosed', 'isLongClosed', 'isNearbyResult', 'latitude', 'localLangCode', 'longitude', 'mealTypes/0', 'mealTypes/1', 
        'mealTypes/2', 'mealTypes/3', 'mealTypes/4', 'nearestMetroStations/0/address', 'nearestMetroStations/0/distance',
        'nearestMetroStations/0/latitude', 'nearestMetroStations/0/lines/0/id', 'nearestMetroStations/0/lines/0/lineName', 
        'nearestMetroStations/0/lines/0/lineSymbol', 'nearestMetroStations/0/lines/0/systemName', 'nearestMetroStations/0/lines/0/systemSymbol',
        'nearestMetroStations/0/lines/0/type', 'nearestMetroStations/0/lines/1/id', 'nearestMetroStations/0/lines/1/lineName', 
        'nearestMetroStations/0/lines/1/lineSymbol', 'nearestMetroStations/0/lines/1/systemName', 'nearestMetroStations/0/lines/1/systemSymbol', 
        'nearestMetroStations/0/lines/1/type', 'nearestMetroStations/0/lines/2/id', 'nearestMetroStations/0/lines/2/lineName', 
        'nearestMetroStations/0/lines/2/lineSymbol', 'nearestMetroStations/0/lines/2/systemName', 'nearestMetroStations/0/lines/2/systemSymbol', 
        'nearestMetroStations/0/lines/2/type', 'nearestMetroStations/0/localAddress', 'nearestMetroStations/0/longitude', 'nearestMetroStations/0/name',
        'nearestMetroStations/1/address', 'nearestMetroStations/1/distance', 'nearestMetroStations/1/latitude', 'nearestMetroStations/1/lines/0/id', 
        'nearestMetroStations/1/lines/0/lineName', 'nearestMetroStations/1/lines/0/lineSymbol', 'nearestMetroStations/1/lines/0/systemName', 
        'nearestMetroStations/1/lines/0/systemSymbol', 'nearestMetroStations/1/lines/0/type', 'nearestMetroStations/1/lines/1/id', 
        'nearestMetroStations/1/lines/1/lineName', 'nearestMetroStations/1/lines/1/lineSymbol', 'nearestMetroStations/1/lines/1/systemName', 
        'nearestMetroStations/1/lines/1/systemSymbol', 'nearestMetroStations/1/lines/1/type', 'nearestMetroStations/1/lines/2/id', 
        'nearestMetroStations/1/lines/2/lineName', 'nearestMetroStations/1/lines/2/lineSymbol', 'nearestMetroStations/1/lines/2/systemName', 
        'nearestMetroStations/1/lines/2/systemSymbol', 'nearestMetroStations/1/lines/2/type', 'nearestMetroStations/1/localAddress', 
        'nearestMetroStations/1/longitude', 'nearestMetroStations/1/name', 'nearestMetroStations/2/address', 'nearestMetroStations/2/distance',  
        'nearestMetroStations/2/latitude', 'nearestMetroStations/2/lines/0/id', 'nearestMetroStations/2/lines/0/lineName', 
        'nearestMetroStations/2/lines/0/lineSymbol', 'nearestMetroStations/2/lines/0/systemName', 'nearestMetroStations/2/lines/0/systemSymbol', 
        'nearestMetroStations/2/lines/0/type', 'nearestMetroStations/2/lines/1/id', 'nearestMetroStations/2/lines/1/lineName', 
        'nearestMetroStations/2/lines/1/lineSymbol', 'nearestMetroStations/2/lines/1/systemName', 'nearestMetroStations/2/lines/1/systemSymbol', 
        'nearestMetroStations/2/lines/1/type', 'nearestMetroStations/2/lines/2/id', 'nearestMetroStations/2/lines/2/lineName', 
        'nearestMetroStations/2/lines/2/lineSymbol', 'nearestMetroStations/2/lines/2/systemName', 'nearestMetroStations/2/lines/2/systemSymbol', 
        'nearestMetroStations/2/lines/2/type', 'nearestMetroStations/2/localAddress', 'nearestMetroStations/2/localName', 'nearestMetroStations/2/longitude', 
        'nearestMetroStations/2/name', 'nearestMetroStations/3/address', 'nearestMetroStations/3/distance', 'nearestMetroStations/3/latitude', 
        'nearestMetroStations/3/lines/0/id', 'nearestMetroStations/3/lines/0/lineName', 'nearestMetroStations/3/lines/0/lineSymbol', 
        'nearestMetroStations/3/lines/0/systemName', 'nearestMetroStations/3/lines/0/systemSymbol', 'nearestMetroStations/3/lines/0/type', 
        'nearestMetroStations/3/lines/1/id', 'nearestMetroStations/3/lines/1/lineName', 'nearestMetroStations/3/lines/1/lineSymbol', 
        'nearestMetroStations/3/lines/1/systemName', 'nearestMetroStations/3/lines/1/systemSymbol', 'nearestMetroStations/3/lines/1/type', 
        'nearestMetroStations/3/lines/2/id', 'nearestMetroStations/3/lines/2/lineName', 'nearestMetroStations/3/lines/2/lineSymbol', 
        'nearestMetroStations/3/lines/2/systemName', 'nearestMetroStations/3/lines/2/systemSymbol', 'nearestMetroStations/3/lines/2/type', 
        'nearestMetroStations/3/localAddress', 'nearestMetroStations/3/localName', 'nearestMetroStations/3/longitude', 'nearestMetroStations/3/name', 
        'nearestMetroStations/4/address', 'nearestMetroStations/4/distance', 'nearestMetroStations/4/latitude', 'nearestMetroStations/4/lines/0/id', 
        'nearestMetroStations/4/lines/0/lineName', 'nearestMetroStations/4/lines/0/lineSymbol', 'nearestMetroStations/4/lines/0/systemName', 
        'nearestMetroStations/4/lines/0/systemSymbol', 'nearestMetroStations/4/lines/0/type', 'nearestMetroStations/4/lines/1/id', 
        'nearestMetroStations/4/lines/1/lineName', 'nearestMetroStations/4/lines/1/lineSymbol', 'nearestMetroStations/4/lines/1/systemName', 
        'nearestMetroStations/4/lines/1/systemSymbol', 'nearestMetroStations/4/lines/1/type', 'nearestMetroStations/4/localAddress', 
        'nearestMetroStations/4/localName', 'nearestMetroStations/4/longitude', 'nearestMetroStations/4/name', 'neighborhoodLocations/0/id', 
        'neighborhoodLocations/0/name', 'neighborhoodLocations/1/id', 'neighborhoodLocations/1/name', 'openNowText', 'orderOnline/0/canProvideTimeslots', 
        'orderOnline/0/headerText', 'orderOnline/0/buttonText', 'orderOnline/0/logoUrl', 'orderOnline/0/offerURL', 'orderOnline/0/provider', 
        'orderOnline/0/providerId', 'orderOnline/0/providerType', 'orderOnline/1/buttonText', 'orderOnline/1/canProvideTimeslots', 'orderOnline/1/headerText', 
        'orderOnline/1/logoUrl', 'orderOnline/1/offerURL', 'orderOnline/1/provider', 'orderOnline/1/providerDisplayName', 'orderOnline/1/providerId', 
        'orderOnline/1/providerType', 'ownersTopReasons', 'photoCount', 'photos/0', 'photos/1', 'photos/2', 'photos/3', 'photos/4', 'photos/5', 'photos/6', 
        'photos/7', 'photos/8', 'photos/9', 'photos/10', 'photos/11', 'photos/12', 'photos/13', 'photos/14', 'photos/15', 'photos/16', 'photos/17', 'photos/18', 
        'photos/19', 'photos/20', 'photos/21', 'photos/22', 'photos/23', 'photos/24', 'photos/25', 'photos/26', 'photos/27', 'photos/28', 'photos/29', 'photos/30', 
        'photos/31', 'photos/32', 'priceRange', 'rankingDenominator', 'rankingPosition', 'subcategories/0', 'travelerChoiceAward', 'type', 'name', 'addressObj/street2', 
        'hours', 'hours/timezone', 'hours/weekRanges/0/0/close', 'hours/weekRanges/0/0/open', 'hours/weekRanges/0/1/close', 'hours/weekRanges/0/1/open', 
        'hours/weekRanges/1/0/close', 'hours/weekRanges/1/0/open', 'hours/weekRanges/1/1/close', 'hours/weekRanges/1/1/open', 'hours/weekRanges/2/0/close', 
        'hours/weekRanges/2/0/open', 'hours/weekRanges/2/1/close', 'hours/weekRanges/2/1/open', 'hours/weekRanges/3/0/close', 'hours/weekRanges/3/0/open', 
        'hours/weekRanges/3/1/close', 'hours/weekRanges/3/1/open', 'hours/weekRanges/4/0/close', 'hours/weekRanges/4/0/open', 'hours/weekRanges/4/1/close', 
        'hours/weekRanges/4/1/open', 'hours/weekRanges/5/0/close', 'hours/weekRanges/5/0/open', 'hours/weekRanges/5/1/close', 'hours/weekRanges/5/1/open', 
        'hours/weekRanges/6/0/close', 'hours/weekRanges/6/0/open', 'hours/weekRanges/6/1/close', 'hours/weekRanges/6/1/open', 'addressObj/street1', 
        'locationString', 'rankingString', 'reviewTags/0/reviews', 'reviewTags/0/text', 'reviewTags/1/reviews', 'reviewTags/1/text', 
        'reviewTags/2/reviews', 'reviewTags/2/text', 'reviewTags/3/reviews', 'reviewTags/3/text', 'reviewTags/4/reviews', 'reviewTags/4/text', 
        'reviewTags/5/reviews', 'reviewTags/5/text', 'reviewTags/6/reviews', 'reviewTags/6/text', 'reviewTags/7/reviews', 'reviewTags/7/text', 
        'reviewTags/8/reviews', 'reviewTags/8/text', 'reviewTags/9/reviews', 'reviewTags/9/text', 'reviewTags/10/reviews', 'reviewTags/10/text', 
        'reviewTags/11/reviews', 'reviewTags/11/text', 'reviewTags/12/reviews', 'reviewTags/12/text', 'ancestorLocations/0/abbreviation', 
        'ancestorLocations/0/id', 'ancestorLocations/0/name', 'ancestorLocations/0/subcategory', 'ancestorLocations/1/abbreviation', 
        'ancestorLocations/1/id', 'ancestorLocations/1/name', 'ancestorLocations/1/subcategory', 'description', 'email', 'establishmentTypes/0', 'establishmentTypes/1'  
        # 添加更多需要刪除的欄位
    ]
    # 刪除不需要欄位
    delete_fields(collection, fields_to_delete)

    # 更新電話格式
    update_phone_format(collection)

    # 更新 district 欄位
    update_district_by_postalcode(collection)

    # 欄位重命名對應字典
    rename_map = {
        "localName": "Restaurant_Name", 
        "rating": "Rating", 
        "numberOfReviews": "Total_Comment", 
        "addressObj/city": "City", 
        "address_translate": "FullAddress", 
        "revised_phone": "Phone_Number",
        "cuisines/0":"Restaurant_Type"
    }

    # 呼叫函數執行欄位重命名
    rename_fields_in_collection(collection, rename_map)

    # 調用函式
    update_district_from_full_address(collection)

    # 獲取並打印欄位名
    print_document_fields(collection)

    # 將清洗後資料存成 CSV
    df = pd.DataFrame(list(collection.find()))
    df.to_csv("tripadvisor_restaurants_cleaning.csv", index=False, encoding='utf-8-sig')
    print("清洗後資料已成功儲存為 CSV 檔案。")

    # 關閉連接
    client.close()

if __name__ == "__main__":
    main()
    upload_to_gcs("tripadvisor_restaurants_cleaning.csv")


