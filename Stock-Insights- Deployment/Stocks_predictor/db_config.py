from pymongo import MongoClient

MONGO_URL = "mongodb+srv://JayKeluskar:JayK@123!@cluster0.qazxav1.mongodb.net/"
MONGO_DB = "stocks_db"

def create_connection():
    client = MongoClient(MONGO_URL)
    db = client[MONGO_DB]
    return db
