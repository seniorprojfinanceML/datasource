import requests
import pandas as pd
from sqlalchemy import create_engine
import os
from datetime import date, datetime, timedelta, timezone
from config import DATABASE_URL

def api():
    url = "https://api.binance.com/api/v3/klines"
    # Currencies
    # symbols = ["BTCUSDT", "ETHUSDT","ADAUSDT","DOGEUSDT","AGIXUSDT", "BNBUSDT"]
    symbols = ["BTCUSDT"]
    # symbols = ["BTCUSDT", "ETHUSDT","ADAUSDT","DOGEUSDT","AGIXUSDT"]
    
    
    # called_date = date.today()
    # current_time = datetime.now().time()
    # # If it is before 12 am., call until 11:59 am. Else, call until 11:59 pm.
    # if current_time < datetime.strptime("12:00:00", "%H:%M:%S").time():
    #     time = datetime.strptime("00:00:00", "%H:%M:%S").time()
    # else:
    #     time = datetime.strptime("12:00:00", "%H:%M:%S").time()

    # combineTime = datetime.combine(called_date, time, tzinfo=timezone.utc)
    for day_i in range(20,21): # 23 29
        data_list = []
        startTime = datetime(year=2021, month=4, day=day_i, hour=9, minute=00, second=0)
        endTime = datetime(year=2021, month=4, day=day_i, hour=11, minute=29, second=0)
        print(startTime, endTime)
        for symbol in symbols:
            params = {
                "symbol": symbol,  # Replace with the trading pair you're interested in
                "interval": "1m", #Replace with the desired time interval (e.g., 1h, 1d, 1w)
                "startTime": int(startTime.timestamp())*1000, #in ms
                "endTime": int(endTime.timestamp())*1000,
                "limit": 1000 # Default is 500, Max is 1000
            }
            response = requests.get(url, params=params)
            if response.status_code == 200:
                kline_data = response.json()
                for kline in kline_data:
                    data_list.append({
                        "time":round(float(kline[0]), 5),
                        "open":round(float(kline[1]), 5),
                        "close":round(float(kline[2]), 5),
                        "low":round(float(kline[3]), 5),
                        "high":round(float(kline[4]), 5),
                        "volume":round(float(kline[5]), 5),
                        "currency":symbol,
                    })
            else:
                print("Error:", response.status_code, response.text)
        # print(data_list)
        data = pd.DataFrame(data_list)
        data["time"]=pd.to_datetime(data["time"]/1000, unit="s")
        data_list.clear()
        print(data)
        print("---------------------------------")
        # insert(data)

def insert(data):
    engine = create_engine(DATABASE_URL)
    data.to_sql("crypto_rounded", engine, if_exists="append", index=False)
    engine.dispose()


if __name__ == "__main__":
    # print(db_params)
    api()