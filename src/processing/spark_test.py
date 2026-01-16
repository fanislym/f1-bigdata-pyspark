import os
import sys

# ---- Windows Spark sanity check ----
if os.name == "nt" and not os.getenv("HADOOP_HOME"):
    print("Windows detected: please set HADOOP_HOME to folder containing bin/winutils.exe")
    sys.exit(1)
# -----------------------------------

from pyspark.sql import SparkSession
from dotenv import load_dotenv
load_dotenv()

def main():
    spark = (
        SparkSession.builder
        .appName("SparkSanityTest")
        .getOrCreate()
    )

    print("Spark version:", spark.version)

    data = [("HAM", 1.2), ("VER", 1.1), ("HAM", 1.3)]
    df = spark.createDataFrame(data, ["Driver", "LapTime"])
    df.groupBy("Driver").avg("LapTime").show()

    spark.stop()

if __name__ == "__main__":
    main()
