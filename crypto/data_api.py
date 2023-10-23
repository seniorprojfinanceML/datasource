import requests
import pandas as pd
from read_csv import insert
import asyncio

async def api():
    url = "https://api.binance.com/api/v3/klines"
    symbols = ["BTCUSDT", "ETHUSDT","ADAUSDT"]
    data_list = []
    currency = {
        "BTCUSDT":"bitcoin",
        "ETHUSDT":"ethereum",
        "ADAUSDT":"cardano",
        "DOGEUSDT":"dogecoin"
    }
    for symbol in symbols:
        params = {
            "symbol": symbol,  # Replace with the trading pair you're interested in
            "interval": "1d",
            # "interval": "1h",     # Replace with the desired time interval (e.g., 1h, 1d, 1w)
            "limit": 1,
        }
        response = requests.get(url, params=params)
        if response.status_code == 200:
            kline_data = response.json()
            for kline in kline_data:
                data_list.append({
                    "time":kline[0],
                    "open":kline[1],
                    "close":kline[2],
                    "low":kline[3],
                    "high":kline[4],
                    "volume":kline[5],
                    "currency":currency[symbol],
                })
        else:
            print("Error:", response.status_code, response.text)
    data = pd.DataFrame(data_list)
    data["time"]=pd.to_datetime(data["time"]/1000, unit="s")
    # print(data)
    return data

if __name__ == "__main__":
    data = asyncio.run(api())
    asyncio.run(insert(data))