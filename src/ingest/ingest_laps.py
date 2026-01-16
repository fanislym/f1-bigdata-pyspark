import os
from datetime import datetime, timezone
from dotenv import load_dotenv
import math
import pandas as pd
import fastf1
from pymongo import MongoClient, UpdateOne

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("MONGO_DB", "f1")
COLL_LAPS = os.getenv("MONGO_COLL_LAPS", "raw_laps")


def to_seconds(td):
    if td is None or pd.isna(td):
        return None
    # if float NaN slips through
    if isinstance(td, float) and math.isnan(td):
        return None
    return float(td.total_seconds())


def ingest_race_laps(year: int, gp_name: str, session: str = "R"):
    if not MONGO_URI:
        raise ValueError("MONGO_URI missing. Create a .env file in repo root.")

    # cache for reproducibility + speed
    fastf1.Cache.enable_cache("data/fastf1_cache")

    s = fastf1.get_session(year, gp_name, session)
    s.load(laps=True, telemetry=False, weather=False)

    laps = s.laps.copy()

    keep_cols = [
        "Driver", "Team", "LapNumber", "Stint", "Compound", "TyreLife",
        "LapTime", "Sector1Time", "Sector2Time", "Sector3Time",
        "PitInTime", "PitOutTime", "IsAccurate", "TrackStatus"
    ]
    laps = laps[keep_cols]

    for c in ["LapTime", "Sector1Time", "Sector2Time", "Sector3Time", "PitInTime", "PitOutTime"]:
        laps[c] = laps[c].apply(to_seconds)

    laps["Year"] = year
    laps["GrandPrix"] = gp_name
    laps["Session"] = session
    laps["IngestedAt"] = datetime.now(timezone.utc).isoformat()

    records = laps.to_dict("records")

    client = MongoClient(MONGO_URI)
    coll = client[DB_NAME][COLL_LAPS]

    ops = []
    for r in records:
        key = {
            "Year": r["Year"],
            "GrandPrix": r["GrandPrix"],
            "Session": r["Session"],
            "Driver": r["Driver"],
            "LapNumber": r["LapNumber"],
        }
        ops.append(UpdateOne(key, {"$set": r}, upsert=True))

    res = coll.bulk_write(ops, ordered=False)
    print(f"Upserted: {res.upserted_count}, Modified: {res.modified_count}, Matched: {res.matched_count}")
    print("Total docs now:", coll.count_documents({}))

if __name__ == "__main__":
    print("Target DB:", os.getenv("MONGO_DB", "f1"))
    print("Target collection:", os.getenv("MONGO_COLL_LAPS", "raw_laps_v2"))
    ingest_race_laps(2023, "Bahrain", "R")

