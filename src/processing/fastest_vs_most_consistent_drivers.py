# src/processing/fastest_vs_most_consistent_drivers.py
"""
Fastest vs Most Consistent Drivers (Spark + MongoDB)

What this script does:
1) Reads lap-level documents from MongoDB using the MongoDB Spark Connector.
2) Computes TWO driver-level views:
   A) Lap-weighted metrics:
      - avg lap time (speed proxy)
      - stddev lap time (consistency proxy; lower = more consistent)
      - lap count
   B) Race-normalized metrics (recommended for fairness):
      - per-race average lap time per driver
      - per-race stddev lap time per driver
      - then averages those per-race metrics across races
      This reduces bias from drivers with uneven lap counts or missing races.

Outputs:
- Prints top fastest drivers
- Prints top most consistent drivers
- Prints a combined "Pareto" table (good speed + good consistency)
"""

import os
import sys
from dotenv import load_dotenv
from pyspark.sql import SparkSession, functions as F


# ---- Windows Spark sanity check ----
if os.name == "nt" and not os.getenv("HADOOP_HOME"):
    print("Windows detected: please set HADOOP_HOME to a folder containing bin/winutils.exe")
    print('Example: setx HADOOP_HOME "C:\\hadoop"')
    sys.exit(1)
# -----------------------------------


def build_spark(mongo_uri: str, db: str, coll: str) -> SparkSession:
    """
    Build a SparkSession configured to read from MongoDB using the connector.
    """
    return (
        SparkSession.builder
        .appName("F1-Fastest-vs-Consistent")
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0")
        .config("spark.mongodb.read.connection.uri", mongo_uri)
        .config("spark.mongodb.read.database", db)
        .config("spark.mongodb.read.collection", coll)
        .getOrCreate()
    )


def load_clean_laps(df):
    """
    Keep only 'clean' laps for fair comparisons:
    - LapTime present and numeric
    - IsAccurate == True (FastF1 quality flag)
    - Exclude pit-in / pit-out laps if those fields exist
    - Keep only race session laps (assumes Session field exists, and 'R' means Race)

    Adjust filters if your schema differs.
    """
    # LapTime validity
    clean = df.filter(F.col("LapTime").isNotNull()).filter(~F.isnan("LapTime"))

    # Optional quality filters if columns exist
    if "IsAccurate" in df.columns:
        clean = clean.filter(F.col("IsAccurate") == True)

    # Optional pit filters if columns exist
    if "PitInTime" in df.columns and "PitOutTime" in df.columns:
        no_pit = (
            (F.col("PitInTime").isNull() | F.isnan("PitInTime")) &
            (F.col("PitOutTime").isNull() | F.isnan("PitOutTime"))
        )
        clean = clean.filter(no_pit)

    # Session filter (Race only)
    if "Session" in df.columns:
        clean = clean.filter(F.col("Session") == F.lit("R"))

    # Required fields
    clean = clean.filter(F.col("Driver").isNotNull())
    if "GrandPrix" in df.columns:
        clean = clean.filter(F.col("GrandPrix").isNotNull())

    return clean


def lap_weighted_metrics(clean_df):
    """
    Lap-weighted driver metrics:
    - avg LapTime across all laps (lower = faster)
    - stddev LapTime across all laps (lower = more consistent)
    - lap count (coverage)
    """
    metrics = (
        clean_df.groupBy("Driver")
        .agg(
            F.count("*").alias("n_laps"),
            F.avg("LapTime").alias("avg_lap_s"),
            F.stddev_pop("LapTime").alias("std_lap_s"),
        )
        .filter(F.col("n_laps") >= 100)  # prevents tiny samples dominating
    )
    return metrics


def race_normalized_metrics(clean_df):
    """
    Race-normalized driver metrics:

    Step 1: Compute per-(Driver, GrandPrix) mean & std.
    Step 2: Aggregate those per-race summaries to driver-level:
            - avg of per-race means  (speed, equal weight per race)
            - avg of per-race stds   (consistency, equal weight per race)
            - races_present          (coverage)
    """
    if "GrandPrix" not in clean_df.columns:
        raise ValueError("GrandPrix column is required for race-normalized metrics.")

    per_race = (
        clean_df.groupBy("Driver", "GrandPrix")
        .agg(
            F.count("*").alias("laps_in_race"),
            F.avg("LapTime").alias("race_avg_lap_s"),
            F.stddev_pop("LapTime").alias("race_std_lap_s"),
        )
        .filter(F.col("laps_in_race") >= 10)  # ensure per-race stats are meaningful
    )

    per_driver = (
        per_race.groupBy("Driver")
        .agg(
            F.count("*").alias("races_present"),
            F.avg("race_avg_lap_s").alias("avg_lap_s_equal_races"),
            F.avg("race_std_lap_s").alias("std_lap_s_equal_races"),
        )
        .filter(F.col("races_present") >= 8)  # reduce bias from partial seasons
    )

    return per_driver


def pareto_table(metrics_df, avg_col: str, std_col: str, top_n: int = 15):
    """
    Build a "Pareto" view: drivers that are both fast and consistent.
    A simple way:
    - Rank drivers by avg (speed) and by std (consistency)
    - Combine ranks to get a composite score (lower is better)
    """
    ranked = (
        metrics_df
        .withColumn("rank_speed", F.dense_rank().over(
            __import__("pyspark").sql.Window.orderBy(F.col(avg_col).asc())
        ))
        .withColumn("rank_consistency", F.dense_rank().over(
            __import__("pyspark").sql.Window.orderBy(F.col(std_col).asc())
        ))
        .withColumn("rank_sum", F.col("rank_speed") + F.col("rank_consistency"))
        .orderBy(F.col("rank_sum").asc(), F.col(avg_col).asc(), F.col(std_col).asc())
    )
    return ranked.limit(top_n)


def main():
    load_dotenv()

    mongo_uri = os.getenv("MONGO_URI")
    db = os.getenv("MONGO_DB", "f1")
    coll = os.getenv("MONGO_COLL_LAPS", "raw_laps_v2")

    if not mongo_uri:
        print("ERROR: MONGO_URI not found in environment. Add it to .env")
        sys.exit(1)

    spark = build_spark(mongo_uri, db, coll)
    df = spark.read.format("mongodb").load()

    clean = load_clean_laps(df)

    # --- A) Lap-weighted metrics ---
    lw = lap_weighted_metrics(clean)

    print("\n=== Lap-weighted: Top 10 Fastest (lowest avg) ===")
    lw.orderBy(F.col("avg_lap_s").asc()).show(10, truncate=False)

    print("\n=== Lap-weighted: Top 10 Most Consistent (lowest stddev) ===")
    lw.orderBy(F.col("std_lap_s").asc()).show(10, truncate=False)

    # Pareto / combined
    print("\n=== Lap-weighted: Best Combined (Speed + Consistency) ===")
    pareto_table(lw, "avg_lap_s", "std_lap_s", top_n=15).show(15, truncate=False)

    # --- B) Race-normalized metrics (recommended) ---
    if "GrandPrix" in clean.columns:
        rn = race_normalized_metrics(clean)

        print("\n=== Race-normalized: Top 10 Fastest (equal weight per race) ===")
        rn.orderBy(F.col("avg_lap_s_equal_races").asc()).show(10, truncate=False)

        print("\n=== Race-normalized: Top 10 Most Consistent (equal weight per race) ===")
        rn.orderBy(F.col("std_lap_s_equal_races").asc()).show(10, truncate=False)

        print("\n=== Race-normalized: Best Combined (Speed + Consistency) ===")
        pareto_table(rn, "avg_lap_s_equal_races", "std_lap_s_equal_races", top_n=15).show(15, truncate=False)
    else:
        print("\n(GrandPrix missing) Skipping race-normalized metrics.")

    # Stop Spark cleanly (important on Windows)
    spark.stop()


if __name__ == "__main__":
    main()
