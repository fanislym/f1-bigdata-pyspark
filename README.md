# Data-Driven Formula 1 — Big Data Management & Processing

Course: COMP-548DL Big Data Management and Processing  
Student: Theofanis Lymbouris (U241N1154)

---

## Project Overview

This project analyzes Formula 1 race performance with a focus on **tyre degradation** and **race strategy** using Big Data management and processing techniques.

Lap-level race data is collected via the **FastF1 API**, ingested into a **NoSQL database (MongoDB Atlas)**, and processed using **Apache Spark (PySpark)** to perform scalable, MapReduce-style analytics.

The pipeline is designed to scale across **multiple races and seasons**, emphasizing realistic Big Data workflows rather than small, in-memory analysis.

---

## Big Data Focus

- **Primary dimension: Volume**
  - Lap-level telemetry data across all races of a season (≈25,000+ documents)
  - Extendable to multiple seasons with no schema changes

- **Secondary dimension: Velocity (simulated)**
  - Incremental, append-only ingestion per race
  - Pipeline supports repeated ingestion without duplication using upserts

- **Data management**
  - MongoDB Atlas (NoSQL, document-based storage)

- **Processing engine**
  - Apache Spark (PySpark)
  - GroupBy and aggregation jobs following the **MapReduce paradigm**

---

## Repository Structure

src/  
├── ingest/  
│   ├── ingest_laps.py  
│   ├── ingest_season_laps.py  
│   └── test_mongo_connection.py  
│  
└── processing/  
    ├── check_counts.py  
    ├── mongo_avg_laptime_by_driver.py  
    ├── tyre_degradation_by_stint.py  
    └── spark_test.py  

---

## Requirements

- Python **3.11**
- Java **11 or 17**
- MongoDB Atlas account **or** local MongoDB instance
- Apache Spark (via PySpark)

---

## Setup

### 1. Create and activate virtual environment (Windows / PowerShell)

py -3.11 -m venv .venv311  
.\.venv311\Scripts\Activate.ps1  
pip install -r requirements.txt  

---

## 2. Configure Environment Variables

Create a `.env` file in the project root (see `.env.example`):

MONGO_URI=mongodb+srv://f1user:LandoNorris1@cluster0.go591zg.mongodb.net/?retryWrites=true&w=majority  
MONGO_DB=f1  
MONGO_COLL_LAPS=raw_laps_v2  

⚠️ **Security note**  
- `.env` is intentionally ignored by `.gitignore`


---

## MongoDB Atlas Access (Evaluator)

For evaluation, a dedicated MongoDB Atlas user with **read and write access** is provided.

- Database: `f1`
- Permissions: `readWrite`
- This allows:
  - Re-running ingestion pipelines
  - Executing Spark processing jobs
  - Verifying data integrity and results

The evaluator only needs to place the provided credentials into the `.env` file as shown above.

---

## 3. Windows-only Spark Requirement

If running on Windows, Hadoop utilities are required for Spark:

setx HADOOP_HOME "C:\hadoop"  
setx PATH "C:\hadoop\bin;%PATH%"

Ensure the following file exists:

C:\hadoop\bin\winutils.exe

If this file is missing, Spark will fail to start on Windows.

Linux and macOS users do **not** need this step.

---

## Running the Project

### 1. Test MongoDB Connection

python src/ingest/test_mongo_connection.py

Expected:
- Successful connection
- Database visibility confirmation

---

### 2. Ingest Race Data (Batch Ingestion)

python src/ingest/ingest_season_laps.py

This ingests all races of the **2023 Formula 1 season** into MongoDB Atlas using **upserts**, preventing duplicate records on re-runs.

---

### 3. Sanity-check Dataset Size

python src/processing/check_counts.py

Expected output:
- ~25,000 lap documents
- 22 race events
- Year: 2023

---

### 4. Run Spark Analytics

**Average lap time per driver:**

python src/processing/mongo_avg_laptime_by_driver.py

**Fastest vs Most Consistent Drivers (Speed–Consistency Tradeoff):**

python src/processing/fastest_vs_most_consistent_drivers.py

**Tyre degradation analysis by stint:**

python src/processing/tyre_degradation_by_stint.py

---

## Notes

- Spark may print Windows-specific warnings when cleaning temporary files  
  → These do **not** affect correctness or results
- MongoDB Spark Connector JARs are downloaded automatically on first run

---

## Big Data Justification

This project demonstrates:

- NoSQL data modeling for semi-structured racing telemetry
- Scalable batch ingestion pipelines
- Distributed data processing using Apache Spark
- MapReduce-style analytics applied to real-world motorsport data

The overall architecture reflects realistic **Big Data management and processing systems** used in modern analytics platforms.

---

**End of README**
