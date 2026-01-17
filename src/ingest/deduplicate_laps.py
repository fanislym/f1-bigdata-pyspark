import os
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DB = os.getenv("MONGO_DB", "f1")
COLL = os.getenv("MONGO_COLL_LAPS", "raw_laps_v2")

client = MongoClient(MONGO_URI)
coll = client[DB][COLL]

print("Deduplicating collection:", DB, ".", COLL)

# Find duplicate keys and list their document ids
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
            "ids": {"$push": "$_id"},
            "n": {"$sum": 1},
        }
    },
    {"$match": {"n": {"$gt": 1}}},
]

dups = list(coll.aggregate(pipeline))
print("Duplicate keys found:", len(dups))

to_delete = []
for d in dups:
    ids = d["ids"]
    # keep the first, delete the rest
    to_delete.extend(ids[1:])

if not to_delete:
    print("No duplicates to delete.")
else:
    res = coll.delete_many({"_id": {"$in": to_delete}})
    print("Deleted duplicate docs:", res.deleted_count)

print("Total docs now:", coll.count_documents({}))
