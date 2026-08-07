# Databricks notebook source
# ============================================
# Gold Layer
# Business metrics and aggregations
# NYC TLC Data Pipeline
# ============================================

from pyspark.sql.functions import (
    col, count, sum, avg, max,
    when, lit, year, month,
    dayofmonth, desc
)

spark.sql("USE CATALOG nyc_tlc")
spark.sql("USE SCHEMA gold")

SILVER_HVFHV = "nyc_tlc.silver.hvfhv"
SILVER_ZONE  = "nyc_tlc.silver.zone_lookup"

print("✅ Gold layer setup complete")

# COMMAND ----------

# ============================================
# Read Silver tables
# ============================================

print("Reading Silver tables...")
silver_df = spark.read.table(SILVER_HVFHV)
zone_df   = spark.read.table(SILVER_ZONE)
print("✅ Silver tables loaded!")

# COMMAND ----------

# ============================================
# Enrich with zone information
# ============================================

print("Joining with zone lookup...")

pickup_zones  = zone_df.alias("pu_zone")
dropoff_zones = zone_df.alias("do_zone")

enriched_df = silver_df \
    .join(
        pickup_zones,
        col("PULocationID") == col("pu_zone.LocationID"),
        "left"
    ) \
    .join(
        dropoff_zones,
        col("DOLocationID") == col("do_zone.LocationID"),
        "left"
    ) \
    .select(
        col("trip_date"),
        col("day_of_week"),
        col("is_weekend"),
        col("PULocationID"),
        col("DOLocationID"),
        col("shared_request_flag"),
        col("shared_match_flag"),
        col("wav_request_flag"),
        col("wav_match_flag"),
        col("is_airport_trip"),
        col("total_amount"),
        col("total_fare"),
        col("trip_miles"),
        col("trip_duration_minutes"),
        col("driver_pay"),
        col("tips"),
        col("pu_zone.borough").alias("pickup_borough"),
        col("pu_zone.zone").alias("pickup_zone"),
        col("pu_zone.service_zone").alias("pickup_service_zone"),
        col("do_zone.borough").alias("dropoff_borough"),
        col("do_zone.zone").alias("dropoff_zone"),
        col("do_zone.service_zone").alias("dropoff_service_zone")
    )

print("✅ Enriched dataset created!")

# COMMAND ----------

# ============================================
# Gold Table 1: Daily Borough Summary
# Grain: day x pickup_borough x dropoff_borough
# ============================================

print("Creating daily_borough table...")

borough_agg = enriched_df.groupBy(
    "trip_date",
    "day_of_week",
    "is_weekend",
    "pickup_borough",
    "dropoff_borough"
).agg(
    count("*").alias("trip_count"),
    sum("total_amount").cast("decimal(12,2)").alias("total_revenue"),
    sum("total_fare").cast("decimal(12,2)").alias("total_fare"),
    sum("tips").cast("decimal(12,2)").alias("total_tips"),
    sum("driver_pay").cast("decimal(12,2)").alias("total_driver_pay"),
    sum("trip_miles").cast("decimal(12,2)").alias("total_distance"),
    avg("trip_miles").cast("decimal(10,2)").alias("avg_trip_distance"),
    avg("trip_duration_minutes").cast("decimal(10,2)").alias("avg_trip_duration"),
    avg("total_amount").cast("decimal(10,2)").alias("avg_fare_per_trip")
).withColumn("year", year("trip_date")) \
 .withColumn("month", month("trip_date"))

borough_agg.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("year", "month") \
    .saveAsTable("nyc_tlc.gold.daily_borough")

print("✅ daily_borough created!")

# COMMAND ----------

# ============================================
# Gold Table 2: Daily Zone Summary
# Grain: day x pickup_zone x dropoff_zone
# ============================================

print("Creating daily_zone table...")

zone_agg = enriched_df.groupBy(
    "trip_date",
    "PULocationID",
    "DOLocationID",
    "is_airport_trip"
).agg(
    count("*").alias("trip_count"),
    sum("total_amount").cast("decimal(12,2)").alias("total_revenue"),
    avg("trip_miles").cast("decimal(10,2)").alias("avg_trip_distance"),
    avg("trip_duration_minutes").cast("decimal(10,2)").alias("avg_trip_duration"),
    avg("total_amount").cast("decimal(10,2)").alias("avg_fare_per_trip"),
    sum("driver_pay").cast("decimal(12,2)").alias("total_driver_pay"),
    max("pickup_zone").alias("pickup_zone"),
    max("pickup_borough").alias("pickup_borough"),
    max("pickup_service_zone").alias("pickup_service_zone"),
    max("dropoff_zone").alias("dropoff_zone"),
    max("dropoff_borough").alias("dropoff_borough"),
    max("dropoff_service_zone").alias("dropoff_service_zone")
).withColumn("year", year("trip_date")) \
 .withColumn("month", month("trip_date")) \
 .withColumn("day", dayofmonth("trip_date"))

zone_agg.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("year", "month", "day") \
    .saveAsTable("nyc_tlc.gold.daily_zone")

print("✅ daily_zone created!")

# COMMAND ----------

# ============================================
# Gold Table 3: Daily Service Summary
# Grain: day x shared_flag x wav_flag
# ============================================

print("Creating daily_service table...")

service_agg = enriched_df.groupBy(
    "trip_date",
    "day_of_week",
    "is_weekend",
    "shared_request_flag",
    "wav_request_flag"
).agg(
    count("*").alias("trip_count"),
    sum("total_amount").cast("decimal(12,2)").alias("total_revenue"),
    avg("total_amount").cast("decimal(10,2)").alias("avg_fare_per_trip"),
    sum(
        when(col("shared_match_flag") == lit(True), lit(1))
        .otherwise(lit(0))
    ).alias("shared_matched_count"),
    sum(
        when(col("wav_match_flag") == lit(True), lit(1))
        .otherwise(lit(0))
    ).alias("wav_matched_count"),
    avg("trip_miles").cast("decimal(10,2)").alias("avg_trip_distance"),
    avg("trip_duration_minutes").cast("decimal(10,2)").alias("avg_trip_duration")
).withColumn("year", year("trip_date")) \
 .withColumn("month", month("trip_date"))

service_agg.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("year", "month") \
    .saveAsTable("nyc_tlc.gold.daily_service")

print("✅ daily_service created!")

# COMMAND ----------

#============================================
# Optimize Gold tables
# ============================================

print("Optimizing Gold tables...")

spark.sql("""
    OPTIMIZE nyc_tlc.gold.daily_borough
    ZORDER BY (trip_date, pickup_borough)
""")

spark.sql("""
    OPTIMIZE nyc_tlc.gold.daily_zone
    ZORDER BY (trip_date, PULocationID)
""")

spark.sql("""
    OPTIMIZE nyc_tlc.gold.daily_service
    ZORDER BY (trip_date, shared_request_flag)
""")

print("✅ All Gold tables optimized!")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'daily_borough' as table_name, COUNT(*) as records
# MAGIC FROM nyc_tlc.gold.daily_borough
# MAGIC UNION ALL
# MAGIC SELECT 'daily_zone', COUNT(*) FROM nyc_tlc.gold.daily_zone
# MAGIC UNION ALL
# MAGIC SELECT 'daily_service', COUNT(*) FROM nyc_tlc.gold.daily_service;