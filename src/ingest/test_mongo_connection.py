import os
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

uri = os.getenv("MONGO_URI")
if not uri:
    raise ValueError("MONGO_URI is missing. Create a .env file in the repo root.")

client = MongoClient(uri)
db = client[os.getenv("MONGO_DB", "f1")]

print("Databases visible:", client.list_database_names())
print("Connected OK. DB object:", db.name)
