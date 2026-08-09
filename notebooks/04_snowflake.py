# Databricks notebook source
# Write Gold tables as parquet to ADLS
GOLD_PATH = (
    "abfss://nyc-tlc@nyctlcdatahubextdl2026"
    ".dfs.core.windows.net/gold/"
)

# COMMAND ----------

# Write daily_borough
print("Writing daily_borough to ADLS...")
borough_df = spark.read.table("nyc_tlc.gold.daily_borough")
borough_df.write \
    .format("parquet") \
    .mode("overwrite") \
    .save(f"{GOLD_PATH}daily_borough/")
print("✅ daily_borough written!")

# COMMAND ----------

# Write daily_zone
print("Writing daily_zone to ADLS...")
zone_df = spark.read.table("nyc_tlc.gold.daily_zone")
zone_df.write \
    .format("parquet") \
    .mode("overwrite") \
    .save(f"{GOLD_PATH}daily_zone/")
print("✅ daily_zone written!")

# COMMAND ----------

# Write daily_service
print("Writing daily_service to ADLS...")
service_df = spark.read.table("nyc_tlc.gold.daily_service")
service_df.write \
    .format("parquet") \
    .mode("overwrite") \
    .save(f"{GOLD_PATH}daily_service/")
print("✅ daily_service written!")