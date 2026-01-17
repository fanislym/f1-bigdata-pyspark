import os
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DB = os.getenv("MONGO_DB", "f1")
COLL = os.getenv("MONGO_COLL_LAPS", "raw_laps_v2")

client = MongoClient(MONGO_URI)
coll = client[DB][COLL]

# how many unique lap keys exist?
pipeline = [
    {
        "$group": {
            "_id": {
                "Year": "$Year",
                "GrandPrix": "$GrandPrix",
                "Session": "$Session",
                "Driver": "$Driver",
                "LapNumber": "$LapNumber",
            },
            "n": {"$sum": 1},
        }
    },
    {"$match": {"n": {"$gt": 1}}},
    {"$count": "duplicate_keys"},
]

result = list(coll.aggregate(pipeline))
dup_keys = result[0]["duplicate_keys"] if result else 0

print("Duplicate lap keys:", dup_keys)
print("Total docs:", coll.count_documents({}))
