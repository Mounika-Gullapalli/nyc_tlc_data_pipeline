# 🚕 NYC TLC High Volume For-Hire Vehicle Analytics Pipeline

## 📋 Project Overview

An end-to-end production-grade batch data pipeline that ingests, transforms and delivers business insights from **560 million NYC taxi trip records** spanning 28 months (January 2024 - April 2026).

This project demonstrates a complete **Modern Data Stack** implementation using Azure cloud services, processing data from Uber, Lyft and other high-volume for-hire vehicle services in New York City.

---

## 🏗️ Architecture

```
NYC TLC Website (Monthly Parquet Files)
         ↓
Azure Data Factory (Ingestion)
├── Historical Pipeline (ForEach - 28 months backfill)
└── Incremental Pipeline (Tumbling Window Trigger - monthly)
         ↓
Azure Data Lake Storage Gen2 (Raw Storage)
└── raw/hvfhv/year=YYYY/month=MM/fhvhv_tripdata_YYYY-MM.parquet
         ↓
Databricks + Delta Lake (Processing)
├── 🥉 Bronze Layer → Raw data as-is (566M records)
├── 🥈 Silver Layer → Cleaned & validated (560M records)
└── 🥇 Gold Layer   → Business metrics (3 aggregation tables)
         ↓
Snowflake (Serving Layer)
├── DAILY_BOROUGH  → Revenue by borough
├── DAILY_ZONE     → Route analytics
└── DAILY_SERVICE  → Service type analysis
         ↓
Databricks Workflows (Orchestration)
└── Bronze → Silver → Gold → Snowflake (automated)
```

---

## 📊 Key Metrics

| Metric | Value |
|--------|-------|
| Raw Records | 566,930,502 |
| Valid Records | 560,615,014 |
| Rejected Records | 6,315,488 |
| Rejection Rate | 1.11% |
| Time Period | Jan 2024 - Apr 2026 |
| Data Size | ~14GB compressed |
| Processing Time | ~1.5 hours (single node) |

---

## 🛠️ Tech Stack

| Component | Technology |
|-----------|-----------|
| Ingestion | Azure Data Factory |
| Storage | Azure Data Lake Storage Gen2 |
| Processing | Databricks + Apache Spark |
| Format | Delta Lake |
| Governance | Unity Catalog |
| Warehouse | Snowflake |
| Orchestration | Databricks Workflows + ADF Triggers |
| Language | Python + SQL |
| Cloud | Azure (Databricks) + AWS (Snowflake) |

---

## 📁 Project Structure

```
nyc-tlc-data-pipeline/
├── README.md
├── notebooks/
│   ├── 01_bronze.py          # Raw data ingestion
│   ├── 02_silver.py          # Cleaning & transformation
│   ├── 03_gold.py            # Business aggregations
│   └── 04_snowflake.py       # Snowflake loading
├── adf/
│   ├── nyc_tlc_ingestion.json    # ADF ingestion pipeline
│   └── nyc_tlc_historical.json   # ADF historical pipeline
├── docs/
│   └── data_dictionary.md    # Column definitions
└── images/
    └── architecture.png      # Architecture diagram
```

---

## 🥉 Bronze Layer

**Purpose:** Store raw data exactly as received from source

- Reads parquet files directly from ADLS Gen2
- No transformations applied
- Partitioned by year and month
- Includes zone lookup reference table

```python
raw_df = spark.read.parquet(RAW_PATH)
raw_df.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .saveAsTable("nyc_tlc.bronze.hvfhv")
```

---

## 🥈 Silver Layer

**Purpose:** Clean, validate and transform Bronze data

### Transformations Applied:
- ✅ Y/N flags → Boolean (True/False)
- ✅ Null handling with coalesce
- ✅ Data type casting (Decimal, Integer, Timestamp)
- ✅ Derived fields (trip_date, day_of_week, is_weekend)
- ✅ Calculated metrics (total_fare, total_amount, avg_speed_mph)
- ✅ Airport trip flag detection
- ✅ Trip duration in minutes

### Validation Rules:
- Pickup and dropoff datetime not null
- Trip distance between 0-500 miles
- Fare amount between $0-$10,000
- Dropoff must be after pickup
- Valid license numbers (HV0002-HV0005)

```python
validated_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("year", "month") \
    .saveAsTable("nyc_tlc.silver.hvfhv")
```

---

## 🥇 Gold Layer

**Purpose:** Business-ready aggregations for analytics

### Table 1: daily_borough
```
Grain: day × pickup_borough × dropoff_borough
Metrics: trip_count, total_revenue, avg_fare, total_distance
Use case: Revenue analysis by borough
```

### Table 2: daily_zone
```
Grain: day × pickup_zone × dropoff_zone × airport_flag
Metrics: trip_count, total_revenue, avg_fare, driver_pay
Use case: Route optimization and demand forecasting
```

### Table 3: daily_service
```
Grain: day × shared_flag × wav_flag × weekend_flag
Metrics: trip_count, total_revenue, shared_matched_count
Use case: Service type analysis and accessibility metrics
```

---

## ❄️ Snowflake Integration

Gold tables loaded into Snowflake for business analytics:

```
NYCTLC_DB.ANALYTICS
├── DAILY_BOROUGH  →     2,553 rows
├── DAILY_ZONE     → 31,220,930 rows
└── DAILY_SERVICE  →    34,802 rows
```

---

## 🔄 Pipeline Orchestration

### ADF Pipelines:
1. **nyc_tlc_historical** - One-time backfill using ForEach
2. **nyc_tlc_ingestion** - Monthly incremental with Tumbling Window trigger

### Databricks Workflow:
```
Task 1: 01_bronze    (reads from ADLS)
    ↓
Task 2: 02_silver    (transforms Bronze)
    ↓
Task 3: 03_gold      (aggregates Silver)
    ↓
Task 4: 04_snowflake (loads Gold to Snowflake)
```

---

## 🔑 Key Design Decisions

### 1. ADF over Databricks for Ingestion
> ADF Copy Activity has zero compute cost for data movement. Using Databricks Spark for simple file downloads wastes expensive DBU compute.

### 2. Medallion Architecture
> Bronze preserves raw data for audit and reprocessing. Silver applies quality checks. Gold pre-aggregates so downstream consumers query small efficient tables instead of 560M raw records.

### 3. Delta Lake over Parquet
> ACID transactions, time travel, schema enforcement and OPTIMIZE for file compaction — essential for production pipelines.

### 4. Partitioning Strategy
> Year/month partitioning + ADLS Gen2 hierarchical namespace enables partition pruning — queries for a specific month read ~500MB instead of 14GB (28x faster).

### 5. Historical vs Incremental Separation
> ForEach pipeline for one-time backfill. Tumbling Window trigger for ongoing monthly ingestion. Clean separation of concerns.

---

## 📈 Data Quality Results

```
Initial records   → 566,930,502
Valid records     → 560,615,014  ✅
Rejected records  →   6,315,488  ❌
Rejection rate    →        1.11% ✅ (industry standard < 5%)
```

### Rejection Reasons:
- Negative trip distances
- Invalid fare amounts (> $10,000)
- Dropoff before pickup timestamp
- Invalid HVFHV license numbers
- Missing required fields

---

## 🔐 Security

- Snowflake credentials stored as environment variables (never hardcoded)
- Azure Managed Identity for ADLS access
- Unity Catalog for data governance and access control
- External locations with storage credentials

---

## 🚀 How to Run

### Prerequisites:
- Azure subscription
- Databricks workspace (Premium tier)
- Snowflake account
- Azure Data Factory

### Setup:

1. **Clone repository:**
```bash
git clone https://github.com/YOUR_USERNAME/nyc-tlc-data-pipeline.git
```

2. **Set up Azure resources:**
- Create ADLS Gen2 storage account
- Create Databricks workspace
- Create ADF instance

3. **Configure connections:**
- Set up storage credentials in Unity Catalog
- Create external location pointing to ADLS
- Configure ADF linked services

4. **Set environment variables:**
```python
import os
os.environ['SF_USERNAME'] = 'your_snowflake_username'
os.environ['SF_PASSWORD'] = 'your_snowflake_password'
os.environ['SF_ACCOUNT']  = 'your_account.snowflakecomputing.com'
```

5. **Run pipelines:**
- Trigger `nyc_tlc_historical` in ADF (one time)
- Activate `nyc_tlc_monthly_trigger` in ADF
- Run Databricks Workflow `nyc_tlc_pipeline`

---

## 💡 Comparison: AWS vs Azure Implementation

| Component | AWS (Original) | Azure (Rebuilt) |
|-----------|---------------|-----------------|
| Storage | S3 | ADLS Gen2 |
| Ingestion | AWS Glue | Azure Data Factory |
| Processing | AWS Glue (Spark) | Databricks |
| Orchestration | Step Functions | ADF + Databricks Workflows |
| Scheduling | EventBridge | Tumbling Window Trigger |
| Incremental | Glue Job Bookmarks | Auto Loader + Checkpointing |
| Governance | Glue Catalog | Unity Catalog |
| Warehouse | N/A | Snowflake |

---

## 📚 Lessons Learned

1. **ADF dataset parameters** cannot access pipeline parameters directly — must pass through dataset-level parameters
2. **Tumbling Window triggers** are ideal for incremental loads but not for historical backfill with partial runs
3. **Single node clusters** process 560M records in ~1.5 hours — production needs multi-node for speed
4. **OPTIMIZE + ZORDER** significantly improves query performance after large writes
5. **Delta Lake overwriteSchema** required when adding partition columns to existing tables

---

## 📄 License

MIT License - feel free to use this project as a reference!
