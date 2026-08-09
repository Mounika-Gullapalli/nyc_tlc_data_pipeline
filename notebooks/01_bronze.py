# Databricks notebook source
# ============================================
# Bronze Layer
# Raw data ingestion into Delta tables
# ============================================

spark.sql("USE CATALOG nyc_tlc")
spark.sql("USE SCHEMA bronze")

# Configuration
RAW_PATH = (
    "abfss://nyc-tlc@nyctlcdatahubextdl2026"
    ".dfs.core.windows.net/raw/hvfhv/"
)

print("✅ Bronze layer setup complete")
print(f"Reading from: {RAW_PATH}")

# COMMAND ----------

# Read raw parquet from ADLS
raw_df = spark.read.parquet(RAW_PATH)

print(f"✅ Total records : {raw_df.count():,}")
print(f"✅ Total columns : {len(raw_df.columns)}")

# COMMAND ----------

# Write to Bronze Delta table
# Raw data as-is — no transformations!
raw_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("year", "month") \
    .saveAsTable("nyc_tlc.bronze.hvfhv")

print("✅ Bronze table created!")

# COMMAND ----------

# Read zone lookup
zone_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(
        "abfss://nyc-tlc@nyctlcdatahubextdl2026"
        ".dfs.core.windows.net/raw/zone/"
        "taxi_zone_lookup.csv"
    )

print(f"✅ Zone records: {zone_df.count():,}")

# Write to Bronze
zone_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("nyc_tlc.bronze.zone_lookup")

print("✅ Bronze zone_lookup created!")
