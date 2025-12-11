import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pandas as pd
from datetime import datetime, timedelta, timezone # 修改這裡
import os
import urllib3

# ==========================================
# 🎯 設定區
# ==========================================
TARGET_URL = "https://soa.tainan.gov.tw/Api/Service/Get/91073f40-d251-42cc-9f4c-88e8937c9911"

# 設定台灣時區 (GitHub 伺服器是 UTC，必須手動修正，否則日期會慢一天)
TW_TIMEZONE = timezone(timedelta(hours=8))
CURRENT_TW_TIME = datetime.now(TW_TIMEZONE)
CSV_FILENAME = f"Tainan_North_Parking_{CURRENT_TW_TIME.strftime('%Y%m%d')}.csv"

def create_session():
    """創建一個具有自動重試功能的連線 Session"""
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

def fetch_and_save_data():
    # 顯示的時間也要改用台灣時間
    current_time_str = datetime.now(TW_TIMEZONE).strftime('%Y-%m-%d %H:%M:%S')
    print(f"\n[{current_time_str}] 正在連線至台南市 SOA 資料庫...")

    try:
        session = create_session()
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        
        response = session.get(TARGET_URL, headers=headers, verify=False, timeout=30)
        response.raise_for_status()
        
        try:
            data = response.json()
        except ValueError:
            print(f"❌ JSON 解析失敗！可能伺服器暫時維護中。")
            return

        if isinstance(data, dict):
             raw_list = data.get('data') or data.get('parkingLots') or [data]
        elif isinstance(data, list):
            raw_list = data
        else:
            raw_list = []

        if not raw_list:
            print("⚠️ 伺服器回傳空資料，本次跳過存檔。")
            return

        df = pd.DataFrame(raw_list)

        col_mapping = {
            'name': 'nameId', 'Name': 'nameId',
            'address': 'address', 'Address': 'address',
            'car_total': 'totalCar', 'TotalSpace': 'totalCar',
            'car': 'availableCar', 'SurplusSpace': 'availableCar',
            'zone': 'district', 'Zone': 'district'
        }
        df.rename(columns=col_mapping, inplace=True)

        if 'address' not in df.columns: df['address'] = ''
        if 'district' not in df.columns: df['district'] = ''
        
        df['address'] = df['address'].astype(str)
        df['district'] = df['district'].astype(str)
        
        mask = df['district'].str.contains('北區', na=False) | df['address'].str.contains('北區|North', na=False, case=False)
        df_north = df[mask].copy()

        if df_north.empty:
            print(f"⚠️ 抓取成功，但篩選後無北區資料 (原始筆數: {len(df)})。")
            return

        for col in ['totalCar', 'availableCar']:
            if col not in df_north.columns:
                df_north[col] = 0
            else:
                df_north[col] = pd.to_numeric(df_north[col], errors='coerce').fillna(0)

        df_north['timestamp'] = current_time_str

        save_cols = ['timestamp', 'nameId', 'address', 'totalCar', 'availableCar']
        final_cols = [c for c in save_cols if c in df_north.columns]
        final_df = df_north[final_cols]
        
        file_exists = os.path.isfile(CSV_FILENAME)
        
        try:
            # 修改編碼為 utf-8 (如果不需Excel直接開，sig可拿掉，但為了中文相容建議保留)
            final_df.to_csv(CSV_FILENAME, mode='a', header=not file_exists, index=False, encoding='utf-8-sig')
            print(f"✅ 成功寫入 {len(final_df)} 筆「北區」資料！")
        except PermissionError:
            print(f"❌ 存檔失敗！(權限錯誤)")
            return

    except requests.exceptions.RequestException as e:
        print(f"❌ 網路連線錯誤: {e}")
    except Exception as e:
        print(f"❌ 發生未預期的錯誤: {e}")

if __name__ == "__main__":
    urllib3.disable_warnings() 
    # 只需要執行一次函式，不需要 while True，也不需要 schedule 套件
    fetch_and_save_data()