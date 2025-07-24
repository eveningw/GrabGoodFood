import pandas as pd
import os
from datetime import datetime

from Utils.gcsHelper import upload_to_gcs

 # 通用函式：將 DataFrame 儲存為 CSV 和 Excel。
def save_dataframe(df, filename_prefix="data", save_csv=True, save_excel=True, folder="", type = ""):
    
    # 確保資料夾存在
    os.makedirs(folder, exist_ok=True)

    # 產生時間戳記
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

    file_name = f"{filename_prefix}_{timestamp}"
    
    # 產生完整檔案路徑
    file_base = os.path.join(folder, file_name)

    if save_excel:
        df.to_excel(f"{file_base}.xlsx", index=False)
        print(f"Excel 檔案已儲存：{file_base}.xlsx")

    if save_csv:
        df.to_csv(f"{file_base}.csv", index=False, encoding="utf-8-sig")
        print(f"CSV 檔案已儲存：{file_base}.csv")

        #存入gcs
        upload_to_gcs(f"{file_base}.csv", type)