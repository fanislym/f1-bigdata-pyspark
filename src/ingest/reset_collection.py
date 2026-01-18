import os
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DB = os.getenv("MONGO_DB", "f1")
COLL = os.getenv("MONGO_COLL_LAPS", "raw_laps_v2")

if not MONGO_URI:
    raise ValueError("MONGO_URI missing")

client = MongoClient(MONGO_URI)
coll = client[DB][COLL]

res = coll.delete_many({})
print(f"Deleted {res.deleted_count} documents from {DB}.{COLL}")
