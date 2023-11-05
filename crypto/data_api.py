import requests
import pandas as pd
from read_csv import insert
import asyncio
import databases
import os, sys
import boto3
import decimal
# from statistics import mean
dir = os.path.dirname(os.getcwd())
sys.path.append(dir)
import config
sys.path.remove(dir)

dynamodb = boto3.client('dynamodb')

async def api():
    url = "https://api.binance.com/api/v3/klines"
    symbols = ["BTCUSDT", "ETHUSDT","ADAUSDT","DOGEUSDT","AGIXUSDT"]
    data_list = []
    currency = {
        "BTCUSDT":"bitcoin",
        "ETHUSDT":"ethereum",
        "ADAUSDT":"cardano",
        "DOGEUSDT":"dogecoin",
        "AGIXUSDT":"singularitynet"
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

async def etl_stock():
    table_name = 'etl-stock'
    dynamodb_resource = boto3.resource('dynamodb')
    table = dynamodb_resource.Table(table_name)
    db = databases.Database(config.DATABASE_URL)
    await db.connect()
    query = """WITH Ranked AS (
            SELECT *, ROW_NUMBER()
            OVER (PARTITION BY stockname ORDER BY datetime DESC) AS row_num
            FROM stock_data
            )
        SELECT * 
        FROM Ranked
        WHERE row_num <= 5;"""
    results = await db.fetch_all(query = query)
    # results = db.fetch_all(query = query)
    data = []
    for result in results:
        data.append(dict(result))
    await db.disconnect()
    # db.disconnect()
    df = pd.DataFrame(data)
    insert_df = pd.DataFrame()
    insert_df['simple_moving_average'] = df.groupby("stockname").mean()["close_price"]
    # insert_df["simple_moving_average"] = insert_df["simple_moving_average"].apply(lambda x: decimal.Decimal(str(x)))
    # print(df)
    # print(df.sort_values(by='datetime',ascending=False ,inplace=False)["datetime"])
    insert_df['insert_time'] = str(list(df.sort_values(by='datetime',ascending=False ,inplace=False)["datetime"])[0])
    insert_df.reset_index(inplace=True)
    data_list = insert_df.to_dict(orient='records')
    insert_list = []
    for data_dict in data_list:
        insert_dict = {
            "insert_datetime": str(data_dict["insert_time"]),
            "stockname": str(data_dict["stockname"]),
            "simple_moving_average": str(data_dict["simple_moving_average"]),
        }
        insert_list.append(insert_dict)
    
    # print(insert_list)
    with table.batch_writer() as batch:
        for item in insert_list:
            batch.put_item(Item=item)
            # pass
    
async def average_recent_crypto():
    query = """WITH ranked_data AS (
        SELECT currency, close, time,
        ROW_NUMBER() OVER (PARTITION BY currency ORDER BY time DESC) AS row_num
        FROM crypto
    )
    SELECT currency, MAX(time) AS time, AVG(close) AS average_data
    FROM ranked_data
    WHERE row_num <= 5
    GROUP BY currency;"""
    db = databases.Database(config.DATABASE_URL)
    await db.connect()
    results = await db.fetch_all(query = query)
    data = []
    for result in results:
        data.append(dict(result))
    print(data)
    await db.disconnect()
    return data
    

async def etl_crypto():
    table_name = 'etl-crypto'
    dynamodb_resource = boto3.resource('dynamodb')
    table = dynamodb_resource.Table(table_name)
    db = databases.Database(config.DATABASE_URL)
    await db.connect()
    query = """WITH Ranked AS (
            SELECT *, ROW_NUMBER()
            OVER (PARTITION BY currency ORDER BY time DESC) AS row_num
            FROM crypto
            )
        SELECT * 
        FROM Ranked
        WHERE row_num <= 5;"""
    results = await db.fetch_all(query = query)
    data = []
    for result in results:
        data.append(dict(result))
    await db.disconnect()
    
    df = pd.DataFrame(data)
    # print(df)
    insert_df = pd.DataFrame()
    insert_df['simple_moving_average'] = df.groupby("currency").mean()["close"]
    insert_df['insert_time'] = str(list(df.sort_values(by='time',ascending=False ,inplace=False)["time"])[0])
    insert_df.reset_index(inplace=True)
    data_list = insert_df.to_dict(orient='records')
    insert_list = []
    for data_dict in data_list:
        insert_dict = {
            "insert_datetime": str(data_dict["insert_time"]),
            "currencyname": str(data_dict["currency"]),
            "simple_moving_average": str(data_dict["simple_moving_average"]),
        }
        insert_list.append(insert_dict)
    
    # print(insert_list)
    with table.batch_writer() as batch:
        for item in insert_list:
            batch.put_item(Item=item)
    
    
    
    ### Might be useful when adding multiple indicators at the same time
    ##  Will choose the method that is more efficient later
    # data_dict = dict()
    # for element in data:
    #     currency = element.pop("currency") 
    #     if currency not in data_dict.keys():
    #         data_dict[currency] = []
    #     data_dict[currency].append(element)
    # # print(data_dict)
    
    # for key, value in data_dict.items():
    #     count = 0
    #     sum = 0
    #     for data_point in value:
    #         sum+=data_point["close"]
    #         count+=1
    #     time = value[0]["time"]
    #     mean = sum/count
    #     output_json = {key:{time:{"ma":mean}}}
    #     # print(value)
    #     print(output_json)

if __name__ == "__main__":
    # data = asyncio.run(api())
    # asyncio.run(insert(data))
    # asyncio.run(etl_crypto())
    asyncio.run(average_recent_crypto())