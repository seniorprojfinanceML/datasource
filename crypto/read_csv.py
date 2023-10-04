import databases
import asyncio
import pandas as pd
from sqlalchemy import create_engine
import os, sys

dir = os.path.dirname(os.getcwd())
sys.path.append(dir)
import config
sys.path.remove(dir)

async def insert(data):
    engine = create_engine(config.DATABASE_URL)
    data.to_sql("crypto", engine, if_exists="append", index=False)
    engine.dispose()

async def read_csv():
    files = os.listdir(".\data")
    data = pd.DataFrame()
    for file in files:
        currency = file.split("_")[0] #Currency name
        # print(file)
        df = pd.read_csv(f".\data\{file}")
        df.drop("End", axis=1, inplace=True)
        df.sort_values(by=["Start"], ascending = True, inplace = True)
        df["currency"] = [currency for row in list(df["Open"])]
        # print(df)
        data = pd.concat([data, df])
    columns = {
            'Start':'time',
            'Open':'open',
            'Close':'close',
            'Low':'low',
            'High':'high',
            'Volume':'volume',
            'Market Cap': 'marketcap'
        }
    data.rename(columns=columns, inplace=True)
    data["time"] = pd.to_datetime(data["time"])
    return data

if __name__ == "__main__":
    data = asyncio.run(read_csv())
    asyncio.run(insert(data))

# async def retrieve_data():
#     db = databases.Database(DATABASE_URL)
#     await db.connect()
#     query = "SELECT * from crypto;"
#     results = await db.fetch_all(query = query)
#     data = []
#     for result in results:
#         data.append(dict(result))
#         # print(dict(result))
#     # print(data)
#     df = pd.DataFrame(data)
#     print(df)
#     await db.disconnect()
#     return df