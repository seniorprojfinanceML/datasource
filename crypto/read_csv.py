import pandas as pd
import csv
from datetime import datetime
import psycopg2
from psycopg2 import sql

import os, sys
dir = os.path.dirname(os.getcwd())
sys.path.append(dir)
import config
sys.path.remove(dir)

import time


def insert_data(currencies):
    table = "crypto"
    query = f"INSERT INTO {table} (currency, time, open, high, low, close, volume) VALUES (%(currency)s, %(time)s, %(open)s, %(high)s, %(low)s, %(close)s, %(volume)s) ON CONFLICT DO NOTHING"
    for currency in currencies:
        dir_path = rf"..\spot\monthly\klines\{currency}\1m"
        files = os.listdir(dir_path)
        for file in files:    
            with open(rf"{dir_path}\{file}", 'r', newline='') as f:
                print(file)
                reader = csv.reader(f)
                data = list(reader)
                # print(data)
                columns = ["time","open","high","low","close","volume"]
                selected_data = [
                    {
                        "currency":currency,
                        "time":datetime.utcfromtimestamp(int(row[0])/1000),
                        "open":row[1],
                        "high":row[2],
                        "low":row[3],
                        "close":row[4],
                        "volume":row[5]
                    }
                    for row in data]
                print("insert")
                # cursor.executemany(query, selected_data)
                # conn.commit()
                # print(selected_data)
                print("done")



if __name__ == "__main__":
    start_time = time.time()
    currencies = ["ETHUSDT","BTCUSDT","BNBUSDT","ADAUSDT", "DOGEUSDT","AGIXUSDT"]
    conn = psycopg2.connect(database="seniorproj_maindb",
    user=config.DATABASE_USERNAME,
    password=config.DATABASE_PASSWORD,
    host=config.DATABASE_HOST,
    port=5432)

    cursor = conn.cursor()
    # cursor.execute("SELECT version();")
    # result = cursor.fetchone()
    # print("PostgreSQL version:", result)
    insert_data(currencies=currencies)

    cursor.close()
    conn.close()
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Elapsed Time: {elapsed_time} seconds")



### Older version
# import databases
# import asyncio
# import pandas as pd
# from sqlalchemy import create_engine
# import os, sys
# dir = os.path.dirname(os.getcwd())
# sys.path.append(dir)
# import config
# sys.path.remove(dir)

# async def insert(data):
#     engine = create_engine(config.DATABASE_URL)
#     data.to_sql("crypto", engine, if_exists="append", index=False)
#     engine.dispose()

# async def read_csv():
#     files = os.listdir(".\data")
#     data = pd.DataFrame()
#     for file in files:
#         currency = file.split("_")[0] #Currency name
#         # print(file)
#         df = pd.read_csv(f".\data\{file}")
#         df.drop("End", axis=1, inplace=True)
#         df.sort_values(by=["Start"], ascending = True, inplace = True)
#         df["currency"] = [currency for row in list(df["Open"])]
#         # print(df)
#         data = pd.concat([data, df])
#     columns = {
#             'Start':'time',
#             'Open':'open',
#             'Close':'close',
#             'Low':'low',
#             'High':'high',
#             'Volume':'volume',
#             'Market Cap': 'marketcap'
#         }
#     data.rename(columns=columns, inplace=True)
#     data["time"] = pd.to_datetime(data["time"])
#     return data

# if __name__ == "__main__":
#     ### Test
#     files = os.listdir("..\spot")
#     print(files)
#     # data = asyncio.run(read_csv())
#     # asyncio.run(insert(data))

# # async def retrieve_data():
# #     db = databases.Database(DATABASE_URL)
# #     await db.connect()
# #     query = "SELECT * from crypto;"
# #     results = await db.fetch_all(query = query)
# #     data = []
# #     for result in results:
# #         data.append(dict(result))
# #         # print(dict(result))
# #     # print(data)
# #     df = pd.DataFrame(data)
# #     print(df)
# #     await db.disconnect()
# #     return df