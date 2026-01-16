import os
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DB = os.getenv("MONGO_DB", "f1")
COLL = os.getenv("MONGO_COLL_LAPS", "raw_laps")

def main():
    client = MongoClient(MONGO_URI)
    coll = client[DB][COLL]

    total = coll.count_documents({})
    events = coll.distinct("EventName")
    years = coll.distinct("Year")

    print("Mongo sanity check")
    print("------------------")
    print(f"Database: {DB}")
    print(f"Collection: {COLL}")
    print(f"Total documents: {total}")
    print(f"Distinct events: {len(events)}")
    print(f"Years present: {sorted(years)}")

if __name__ == "__main__":
    main()
