import sys, os
import pymongo
import databases
import asyncio
from datetime import datetime, timedelta
dir = os.path.dirname(os.getcwd())
sys.path.append(dir)
import config
sys.path.remove(dir)

# print(config.MONGODB)
connection_string = config.MONGODB
database_name = "etl"
collection_name = "crypto"


async def query_data():
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
    return data

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
    # print(data)
    await db.disconnect()
    return data

def sample_delete(collection):
    time = datetime.now() - timedelta(days=2)
    print(time)
    collection.delete_many({"time":{"$gte":time}})

def sample_query(collection):
    return collection.find({"average_data":{"$gte":1,"$lte":10000}})

if __name__ == "__main__":
    client = pymongo.MongoClient(connection_string)
    # print(data)
    db = client[database_name]
    collection = db[collection_name]
    data = asyncio.run(average_recent_crypto())
    result = collection.insert_many(data)
    # sample_delete(collection)
    
    # results = sample_query(collection)
    # for result in results:
    #     print(result)
    

    client.close()