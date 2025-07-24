import os
import signal
import google.generativeai as genai
from http.client import HTTPException
import threading

# 設定請求超時的秒數
TIMEOUT = 100  

class TimeoutException(Exception):
    pass

# Get text data from response
def get_ai_response_text(response):
    try:
        response_content = ""
        if response.candidates and response.candidates[0].content.parts:
            response_content = response.candidates[0].content.parts[0].text
            # print(response_content)
            # print(response)
        
    except HTTPException as e:
        print(f"get_ai_response_text HTTPException occurred: {e}")
        response_content = ""
    except Exception as e:
        print(f"get_ai_response_text An error occurred: {e}")
        response_content = ""
    return response_content

# 沒在使用的話isWork 都要設false
def get_ai_response(restaurant_input, type, is_Work: False):

    if is_Work:

        # key 在地端.env檔(不要進版控)
        google_api_key = os.getenv("GOOGLE_API_KEY")

        genai.configure(api_key=google_api_key)

        # Create the model
        generation_config = {
        "temperature": 0.5,
        "top_p": 0.95,
        "top_k": 64,
        "max_output_tokens": 40,
        "response_mime_type": "text/plain",
        } 

        system_instructions = { 
                "generate_restaurant_main_type_tag":
                """「請根據以下分類，判斷我提供的餐廳名稱所屬的大類和中類。若有多種餐飲類型，選最符合的。若類型不在分類中，判斷最接近的選項：
                大項： 台灣菜，中式料理，亞洲料理，日本料理，西式料理

                中項： 台菜, 小吃, 鐵板燒, 客家餐廳, 海鮮餐廳, 火鍋, 早午餐, 素食餐廳, 中式, 四川餐廳, 粵菜餐廳, 中式麵食店, 滬菜餐廳, 港式餐廳, 浙江餐廳, 湘菜餐廳, 江浙餐廳, 福建餐廳, 北京餐廳, 陝西餐廳, 湖南餐廳, 雲南料理, 印度餐廳, 泰國餐廳, 馬來西亞餐廳, 越南餐廳, 異國料理, 新加坡餐廳, 韓國餐廳, 日式料理, 拉麵, 居酒屋, 定食, 日式咖哩, 壽司, 丼飯, 日式西餐廳, 鰻魚, 壽喜燒, 懷石料理, 義大利餐廳, 美式餐廳, 多國餐廳, 甜點輕食, 西餐廳, 餐酒館, 小餐館 (Bistro), 美式牛排, 法國餐廳, 比薩, 地中海餐廳, 自助餐, 西班牙餐廳, 德國餐廳, 咖啡輕食, 
                範例：
                顯示方式: 大類-中類
                餐廳名稱：「波記私宅打邊爐（台北東區港式美食推薦)」 -> 中式料理-火鍋
                餐廳名稱：「一蘭拉麵」 -> 日本料理-拉麵
                餐廳名稱：「Subway」-> 西式料理-美式餐廳
                餐廳名稱：「Chizup! 美式濃郁起司蛋糕」-> 西式料理-甜點輕食
                        """,
                
                "generate_restaurant_type_tag": 
                "只輸出餐廳的一種主要類別名稱，不要列表符號 (*)、換行 (\n) 或額外描述。例如：美式 或 日式料理。輸出內容請確保不超過 5 個字。",
                                
                "generate_restaurant_name_tag": 
                """請先判斷餐廳名稱，然後保留完整的餐廳名稱，包含分店名稱與地區，移除餐廳名稱後面的多於敘述
                正確:輸入:東引快刀手 內湖店 -(人氣排隊美食); 輸出:東引快刀手 內湖店; 
                錯誤:輸入: 寧夏夜市千歲宴; 輸出:千歲宴"""
            }

        prompts = {
            "restaurant_main_type": f"{restaurant_input}是什麼類型，輸出一個大類別及一個中類別",
            "restaurant_name": f"{restaurant_input} 的餐廳名稱",
            "restaurant_type" : f"{restaurant_input}是什麼類型的餐廳"
            }

        #取得餐廳類別
        if type =="get_type":

            gemini_model = genai.GenerativeModel(
            model_name="models/gemini-2.0-flash",
            # model_name="gemini-2.0-flash-lite-preview-02-05",
            generation_config=generation_config,
            system_instruction=system_instructions["generate_restaurant_type_tag"]
            )
            
            restaurant_type_response = gemini_model.generate_content(prompts["restaurant_type"])
            restaurant_type_text = get_ai_response_text(restaurant_type_response)

            return restaurant_type_text
            
        #取得餐廳名稱
        if type == "get_name":
            gemini_model = genai.GenerativeModel(
            model_name="models/gemini-2.0-flash",
            generation_config=generation_config,
            system_instruction=system_instructions["generate_restaurant_name_tag"]
            )

            restaurant_name_response = gemini_model.generate_content(prompts["restaurant_name"])
            restaurant_name__text = get_ai_response_text(restaurant_name_response)

            return restaurant_name__text
        
        #取得餐廳大類型
        if type == "get_main_type":
            gemini_model = genai.GenerativeModel(
            model_name="models/gemini-2.0-flash",
            generation_config=generation_config,
            system_instruction=system_instructions["generate_restaurant_main_type_tag"]
            )

            restaurant_main_type_response = gemini_model.generate_content(prompts["restaurant_main_type"])
            restaurant_main_type_text = get_ai_response_text(restaurant_main_type_response)

            return restaurant_main_type_text
        
    # is work false 就回傳空值   
    return ""

def execute_ai_model(restaurant_input, type, is_Work: False):
    def run_model():
        nonlocal response
        try:
            response = get_ai_response(restaurant_input, type, is_Work)
        except Exception as e:
            print(f"An error occurred: {e}")
            response = ""

    response = ""
    thread = threading.Thread(target=run_model)
    thread.start()
    thread.join(TIMEOUT)

    # 對應timeout的狀況
    if thread.is_alive():
        print(f"{restaurant_input}:TimeoutException occurred: Google AI API request timed out.")
        return ""
    return response

# 測試用
# if __name__ == "__main__":

#     name = execute_ai_model("新高軒信義店(A11-B2)","get_name", True)

#     type = execute_ai_model(name,"get_type", True)

#     main_type = execute_ai_model(name,"get_main_type", True)

#     print(name)
#     print(type)
#     print(main_type)
    

    
