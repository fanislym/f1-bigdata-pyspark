import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession, functions as F
import sys

# ---- Windows Spark sanity check ----
if os.name == "nt" and not os.getenv("HADOOP_HOME"):
    print("Windows detected: please set HADOOP_HOME to a folder containing bin/winutils.exe")
    sys.exit(1)
# -----------------------------------

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DB = os.getenv("MONGO_DB", "f1")
COLL = os.getenv("MONGO_COLL_LAPS", "raw_laps")

def main():
    spark = (
        SparkSession.builder
        .appName("F1MongoAvgLapTime")
        # MongoDB Spark Connector (Spark 3/4, Scala 2.12)
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0")
        .config("spark.mongodb.read.connection.uri", MONGO_URI)
        .config("spark.mongodb.read.database", DB)
        .config("spark.mongodb.read.collection", COLL)
        .getOrCreate()
    )

    df = spark.read.format("mongodb").load()

    # LapTime is stored as seconds from your mongo_safe()
    res = (
        df.filter(F.col("LapTime").isNotNull())
        .groupBy("Driver")
        .agg(
            F.count("*").alias("laps"),
            F.avg("LapTime").alias("avg_lap_s")
        )
        .orderBy(F.col("avg_lap_s").asc())
    )


    res.show(50, truncate=False)

    # Stop Spark cleanly (important on Windows)
    #spark.sparkContext.stop()
    spark.stop()


if __name__ == "__main__":
    main()
