import os
import sys

# ---- Windows Spark sanity check ----
if os.name == "nt" and not os.getenv("HADOOP_HOME"):
    print("Windows detected: please set HADOOP_HOME to folder containing bin/winutils.exe")
    sys.exit(1)
# -----------------------------------

from dotenv import load_dotenv
from pyspark.sql import SparkSession, functions as F

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DB = os.getenv("MONGO_DB", "f1")
COLL = os.getenv("MONGO_COLL_LAPS", "raw_laps")

def main():
    spark = (
        SparkSession.builder
        .appName("F1TyreDegradationByStint")
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0")
        .config("spark.mongodb.read.connection.uri", MONGO_URI)
        .config("spark.mongodb.read.database", DB)
        .config("spark.mongodb.read.collection", COLL)
        .getOrCreate()
    )

    df = spark.read.format("mongodb").load()

    # Treat NaN as missing for pit times
    no_pit = (
        (F.col("PitInTime").isNull() | F.isnan("PitInTime")) &
        (F.col("PitOutTime").isNull() | F.isnan("PitOutTime"))
    )

    clean = (
        df
        .filter(F.col("LapTime").isNotNull())
        .filter(~F.isnan("LapTime"))
        .filter(F.col("TyreLife").isNotNull())
        .filter(~F.isnan("TyreLife"))
        .filter(F.col("Stint").isNotNull())
        .filter(F.col("Driver").isNotNull())
        .filter(F.col("GrandPrix").isNotNull())
        .filter(F.col("Compound").isin(["SOFT", "MEDIUM", "HARD"]))
        .filter(F.col("IsAccurate") == True)
        .filter(no_pit)
        .filter(F.col("TyreLife") >= 2)  # drop first tyre lap (often noisy)
    )

    # x = tyre age (laps), y = laptime (seconds)
    x = F.col("TyreLife").cast("double")
    y = F.col("LapTime").cast("double")

    agg = (
        clean
        .groupBy("Year", "GrandPrix", "Session", "Driver", "Stint", "Compound")
        .agg(
            F.count("*").alias("n_laps"),
            F.avg(x).alias("x_mean"),
            F.avg(y).alias("y_mean"),
            F.avg(x * y).alias("xy_mean"),
            F.avg(x * x).alias("x2_mean"),
            F.min("TyreLife").alias("tyre_life_min"),
            F.max("TyreLife").alias("tyre_life_max"),
        )
    )

    res = (
        agg
        .withColumn("cov_xy", F.col("xy_mean") - (F.col("x_mean") * F.col("y_mean")))
        .withColumn("var_x", F.col("x2_mean") - (F.col("x_mean") * F.col("x_mean")))
        .withColumn(
            "deg_s_per_tyre_lap",
            F.when(F.col("var_x") == 0, F.lit(None)).otherwise(F.col("cov_xy") / F.col("var_x"))
        )
        .withColumn("deg_ms_per_tyre_lap", F.col("deg_s_per_tyre_lap") * F.lit(1000.0))
        .filter(F.col("n_laps") >= 8)
        .orderBy(F.col("deg_ms_per_tyre_lap").desc())
    )

    res.show(50, truncate=False)

    spark.stop()

if __name__ == "__main__":
    main()
