import os
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DB = os.getenv("MONGO_DB", "f1")
COLL = os.getenv("MONGO_COLL_LAPS", "raw_laps_v2")


def main():
    if not MONGO_URI:
        raise ValueError("MONGO_URI missing. Create a .env file in repo root.")

    client = MongoClient(MONGO_URI)
    coll = client[DB][COLL]

    total = coll.count_documents({})
    years = sorted(coll.distinct("Year"))

    # Your ingestion uses GrandPrix + Session
    gps = coll.distinct("GrandPrix")
    sessions = coll.distinct("Session")

    # Basic schema sanity: how many docs miss key fields?
    missing_grandprix = coll.count_documents({"GrandPrix": {"$exists": False}})
    missing_driver = coll.count_documents({"Driver": {"$exists": False}})
    missing_laptime = coll.count_documents({"LapTime": {"$exists": False}})

    print("Mongo sanity check")
    print("------------------")
    print(f"Database: {DB}")
    print(f"Collection: {COLL}")
    print(f"Total documents: {total}")
    print(f"Distinct GrandPrix events: {len(gps)}")
    print(f"Sessions present: {sorted(sessions)}")
    print(f"Years present: {years}")
    print("")
    print("Missing-field checks")
    print("--------------------")
    print(f"Docs missing GrandPrix: {missing_grandprix}")
    print(f"Docs missing Driver:   {missing_driver}")
    print(f"Docs missing LapTime:  {missing_laptime}")


if __name__ == "__main__":
    main()
