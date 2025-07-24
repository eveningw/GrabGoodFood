import re

def mapping_address(address):
    # 嘗試從地址中匹配縣市和區域
    city = ""
    district = ""
    postal_code = ""    
    if not address:
        return "", ""
    # 已知縣市與區域的列表
    cities = ['台北市', '新北市', '高雄市', '台中市', '台南市', '基隆市', '桃園市', '台中市', '新竹市', '苗栗市', '彰化市', '南投市', '屏東市', '宜蘭市', '花蓮市', '澎湖市', '金門市']
    # 區域及對應的郵遞區號
    districts = {
        "北投區": "112", "士林區": "111", "大同區": "103", "中山區": "104", "松山區": "105", 
        "內湖區": "114", "萬華區": "108", "中正區": "100", "大安區": "106", "信義區": "110", 
        "南港區": "115", "文山區": "116"
    }

    # 匹配縣市
    for one_city in cities:
        if one_city in address:
            city = one_city
            break
    
    # 匹配區域 
    for one_district, code in districts.items():
        if one_district in address:
            district = one_district
            postal_code = code
            break

    return postal_code, city, district

def extract_address(address_str):
    try:

        match = re.match(r'(\d{3,6})?([\u4e00-\u9fa5]+[市縣])([\u4e00-\u9fa5]+區)(.*)', address_str)
        city = ""
        district = ""
        postal_code = ""
        if match:
            postal_code = match.group(1).strip()
            city = match.group(2).strip()
            district = match.group(3).strip()
            # print(f"{city}, {district}, {address_str}")
        else: 
            postal_code, city, district = mapping_address(address_str)
            # print({city}, {district}, {address_str})
        
    except Exception as e:
         print(f"extract_address 錯誤 : {e}")

    finally:
        return postal_code, city, district 

# if __name__ == "__main__":
#     # address = "No. 34 No, No. 34號葫蘆街士林區台北市111"
#     # address = "100台北市中正區八德路一段1號華山文創園區 中3A-2"

#     # 測試案例
#     addresses = [
#         "100台北市中正區八德路一段1號華山文創園區 中3A-2",
#         "220新北市板橋區文化路一段100號",
#         "高雄市三民區建國路100號",
#         "桃園縣中壢區中山路22號",
#         "台中市西屯區文心路三段33號",
#         "彰化縣和美區鹿港路88號"
#     ]
#     for address in addresses:
#         city, distict = extract_address(address)
#         print(city, distict, address)