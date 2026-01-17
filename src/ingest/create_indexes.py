import os
from dotenv import load_dotenv
from pymongo import MongoClient, ASCENDING

load_dotenv()

client = MongoClient(os.getenv("MONGO_URI"))
db = client[os.getenv("MONGO_DB", "f1")]
coll = db[os.getenv("MONGO_COLL_LAPS", "raw_laps_v2")]

print("Creating UNIQUE index on lap key...")

coll.create_index(
    [
        ("Year", ASCENDING),
        ("GrandPrix", ASCENDING),
        ("Session", ASCENDING),
        ("Driver", ASCENDING),
        ("LapNumber", ASCENDING),
    ],
    unique=True,
    name="uniq_lap_key"
)

print("Done.")
