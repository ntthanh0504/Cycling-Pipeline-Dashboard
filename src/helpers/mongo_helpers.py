import pymongo
from pymongo import MongoClient
from typing import List

MONGO_URI = "mongodb://thanh:123@mongodb:27017/?authSource=admin"
def init_client(uri: str):
    client = MongoClient(uri)
    return client

def check_collection_exists(db_name: str, collection_name: str):
    client = init_client(MONGO_URI)
    db = client[db_name]
    collection_names = db.list_collection_names()
    client.close()
    return collection_name in collection_names

def upsert_data(db_name: str, collection_name: str, data: List[dict]):
    client = init_client(MONGO_URI)
    db = client[db_name]
    collection = db[collection_name]
    
    for record in data:
        collection.update_one(
            {"_id": record["_id"]},
            {"$set": record},
            upsert=True
        )
    client.close()

def insert_data(db_name: str, collection_name: str, data: List[dict]):
    client = init_client(MONGO_URI)
    db = client[db_name]
    collection = db[collection_name]
    
    collection.insert_many(data)
    client.close()
    
def fetch_data(db_name: str, collection_name: str, query: dict, projection: dict = None):
    client = init_client(MONGO_URI)
    db = client[db_name]
    collection = db[collection_name]
    
    results = collection.find(query, projection)
    client.close()
    return list(results)

