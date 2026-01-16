import os
from datetime import datetime, UTC
from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne
import fastf1
from tqdm import tqdm
import pandas as pd
import numpy as np
import math

def mongo_safe(value):
    if value is None:
        return None

    # pandas missing
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass

    # float NaN
    if isinstance(value, float) and math.isnan(value):
        return None

    # numpy NaN
    if isinstance(value, np.floating) and np.isnan(value):
        return None

    # Timedelta -> seconds
    if isinstance(value, pd.Timedelta):
        return value.total_seconds()

    # Timestamp -> ISO string
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime().isoformat()

    # numpy scalar -> python scalar
    if isinstance(value, (np.integer, np.floating, np.bool_)):
        return value.item()

    return value

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
if not MONGO_URI:
    raise ValueError("MONGO_URI missing. Create a .env file in repo root.")

DB_NAME = os.getenv("MONGO_DB", "f1")
COLL_NAME = os.getenv("MONGO_COLL_LAPS", "raw_laps")

def ingest_race_laps(year: int, gp_name: str, session: str = "R") -> int:
    """
    Ingest lap data for one race into MongoDB.
    Uses upsert to avoid duplicates if re-run.
    Returns number of upsert operations executed.
    """
    fastf1.Cache.enable_cache("data/fastf1_cache")

    client = MongoClient(MONGO_URI)
    coll = client[DB_NAME][COLL_NAME]

    sess = fastf1.get_session(year, gp_name, session)
    sess.load()

    laps = sess.laps.reset_index(drop=True)
    keep_cols = [
        "Driver","Team","LapNumber","Stint","Compound","TyreLife",
        "LapTime","Sector1Time","Sector2Time","Sector3Time",
        "PitInTime","PitOutTime","IsAccurate","TrackStatus"
    ]
    laps = laps[keep_cols]

    # Convert to JSON-friendly format
    laps_dicts = laps.to_dict(orient="records")

    now = datetime.now(UTC).isoformat()

    ops = []
    for row in laps_dicts:
        # A stable unique key to prevent duplicates:
        # Year + EventName + SessionName + Driver + LapNumber
        key = {
        "Year": year,
        "GrandPrix": sess.event["EventName"],
        "Session": session,
        "Driver": row.get("Driver"),
        "LapNumber": row.get("LapNumber"),
        }


        clean_row = {k: mongo_safe(v) for k, v in row.items()}

        clean_row["Year"] = year
        clean_row["GrandPrix"] = sess.event["EventName"]
        clean_row["Session"] = session
        clean_row["IngestedAt"] = now

        ops.append(UpdateOne(key, {"$set": clean_row}, upsert=True))



    if not ops:
        print(f"No laps found for {year} {gp_name} {session}")
        return 0

    result = coll.bulk_write(ops, ordered=False)
    upserts = result.upserted_count
    modified = result.modified_count
    matched = result.matched_count

    print(
        f"{year} | {gp_name} | {sess.event['EventName']} | {sess.name} -> "
        f"Upserted: {upserts}, Modified: {modified}, Matched: {matched}"
    )

    return len(ops)


def ingest_season(year: int = 2023):
    # Get official event schedule for the season
    schedule = fastf1.get_event_schedule(year)
    # Keep only real races (not testing)
    races = schedule[schedule["EventFormat"] != "testing"]

    print(f"Found {len(races)} events for {year}")

    ok = 0
    failed = []

    for _, ev in tqdm(races.iterrows(), total=len(races)):
        # FastF1 can accept EventName or GP name; EventName is usually safest.
        event_name = ev["EventName"]

        try:
            ingest_race_laps(year, event_name, "R")
            ok += 1
        except Exception as e:
            failed.append((event_name, str(e)))
            print(f"FAILED: {event_name} -> {e}")

    print(f"\nDone. Success: {ok}/{len(races)}")
    if failed:
        print("\nFailures:")
        for name, err in failed:
            print(f"- {name}: {err}")


if __name__ == "__main__":
    ingest_season(2023)
