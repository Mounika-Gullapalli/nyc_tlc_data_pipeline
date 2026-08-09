# Databricks notebook source
# ============================================
# Silver Layer
# Clean, validate and transform Bronze data
# NYC TLC Data Pipeline
# ============================================

from pyspark.sql.functions import (
    col, when, lit, hour, month, year,
    dayofweek, dayofmonth, coalesce,
    current_timestamp
)
from pyspark.sql.types import *

spark.sql("USE CATALOG nyc_tlc")
spark.sql("USE SCHEMA silver")

print("✅ Silver layer setup complete")

# COMMAND ----------

# ============================================
# Read from Bronze Delta table
# ============================================

print("Reading Bronze table...")
bronze_df = spark.read.table("nyc_tlc.bronze.hvfhv")
print("✅ Bronze table loaded!")

# COMMAND ----------

# ============================================
# Transformations
# ============================================

def transform_bronze_to_silver(df):
    print("Applying transformations...")

    silver_df = df.select(

        # Direct passthrough fields
        col("hvfhs_license_num"),
        col("dispatching_base_num"),
        col("originating_base_num"),

        # Timestamp fields
        col("request_datetime").cast("timestamp"),
        col("on_scene_datetime").cast("timestamp"),
        col("pickup_datetime").cast("timestamp"),
        col("dropoff_datetime").cast("timestamp"),

        # Location fields
        col("PULocationID").cast("integer"),
        col("DOLocationID").cast("integer"),

        # Numeric fields with null handling
        coalesce(col("trip_miles"), lit(0.0))
            .cast("decimal(10,2)").alias("trip_miles"),
        col("trip_time").cast("integer")
            .alias("trip_time_seconds"),
        coalesce(col("base_passenger_fare"), lit(0.0))
            .cast("decimal(10,2)").alias("base_passenger_fare"),
        coalesce(col("tolls"), lit(0.0))
            .cast("decimal(10,2)").alias("tolls"),
        coalesce(col("bcf"), lit(0.0))
            .cast("decimal(10,2)").alias("bcf"),
        coalesce(col("sales_tax"), lit(0.0))
            .cast("decimal(10,2)").alias("sales_tax"),
        coalesce(col("congestion_surcharge"), lit(0.0))
            .cast("decimal(10,2)").alias("congestion_surcharge"),
        coalesce(col("airport_fee"), lit(0.0))
            .cast("decimal(10,2)").alias("airport_fee"),
        coalesce(col("tips"), lit(0.0))
            .cast("decimal(10,2)").alias("tips"),
        coalesce(col("driver_pay"), lit(0.0))
            .cast("decimal(10,2)").alias("driver_pay"),

        # Boolean flag conversions Y/N → True/False
        when(col("shared_request_flag") == "Y", True)
            .otherwise(False).alias("shared_request_flag"),
        when(col("shared_match_flag") == "Y", True)
            .otherwise(False).alias("shared_match_flag"),
        when(col("access_a_ride_flag") == "Y", True)
            .otherwise(False).alias("access_a_ride_flag"),
        when(col("wav_request_flag") == "Y", True)
            .otherwise(False).alias("wav_request_flag"),
        when(col("wav_match_flag") == "Y", True)
            .otherwise(False).alias("wav_match_flag"),

        # Derived fields
        col("pickup_datetime").cast("date").alias("trip_date"),
        hour("pickup_datetime").alias("pickup_hour"),

        # Day of week as string
        when(dayofweek("pickup_datetime") == 1, "Sunday")
        .when(dayofweek("pickup_datetime") == 2, "Monday")
        .when(dayofweek("pickup_datetime") == 3, "Tuesday")
        .when(dayofweek("pickup_datetime") == 4, "Wednesday")
        .when(dayofweek("pickup_datetime") == 5, "Thursday")
        .when(dayofweek("pickup_datetime") == 6, "Friday")
        .when(dayofweek("pickup_datetime") == 7, "Saturday")
        .otherwise("Unknown").alias("day_of_week"),

        # Weekend flag
        when(
            dayofweek("pickup_datetime").isin([1, 7]), True
        ).otherwise(False).alias("is_weekend"),

        # Trip duration in minutes
        (col("trip_time") / 60.0)
            .cast("decimal(10,2)").alias("trip_duration_minutes"),

        # Total fare calculation
        (
            coalesce(col("base_passenger_fare"), lit(0.0)) +
            coalesce(col("tolls"), lit(0.0)) +
            coalesce(col("bcf"), lit(0.0)) +
            coalesce(col("sales_tax"), lit(0.0)) +
            coalesce(col("congestion_surcharge"), lit(0.0)) +
            coalesce(col("airport_fee"), lit(0.0))
        ).cast("decimal(10,2)").alias("total_fare"),

        # Total amount including tips
        (
            coalesce(col("base_passenger_fare"), lit(0.0)) +
            coalesce(col("tolls"), lit(0.0)) +
            coalesce(col("bcf"), lit(0.0)) +
            coalesce(col("sales_tax"), lit(0.0)) +
            coalesce(col("congestion_surcharge"), lit(0.0)) +
            coalesce(col("airport_fee"), lit(0.0)) +
            coalesce(col("tips"), lit(0.0))
        ).cast("decimal(10,2)").alias("total_amount"),

        # Average speed
        when(
            col("trip_time") > 0,
            (col("trip_miles") / (col("trip_time") / 3600.0))
        ).otherwise(None).cast("decimal(10,2)").alias("avg_speed_mph"),

        # Airport trip flag
        when(
            coalesce(col("airport_fee"), lit(0.0)) > 0, True
        ).otherwise(False).alias("is_airport_trip"),

        # Partition columns
        year("pickup_datetime").alias("year"),
        month("pickup_datetime").alias("month"),
        dayofmonth("pickup_datetime").alias("day"),

        # Processing timestamp
        current_timestamp().alias("processing_timestamp")
    )

    print("✅ Transformations applied!")
    return silver_df


# Apply transformations
silver_df = transform_bronze_to_silver(bronze_df)

# COMMAND ----------

# ============================================
# Data Validation
# ============================================
def validate_silver_data(df):
    print("Validating data...")

    initial_count = df.count()

    validated_df = df.filter(
        col("pickup_datetime").isNotNull() &
        col("dropoff_datetime").isNotNull() &
        col("PULocationID").isNotNull() &
        col("DOLocationID").isNotNull() &
        (col("trip_miles") >= 0) &
        (col("trip_miles") <= 500) &
        (col("base_passenger_fare") >= 0) &
        (col("base_passenger_fare") <= 10000) &
        (col("dropoff_datetime") > col("pickup_datetime")) &
        (col("pickup_datetime") >= col("request_datetime")) &
        col("hvfhs_license_num").isin([
            'HV0002', 'HV0003', 'HV0004', 'HV0005'
        ])
    )

    validated_count = validated_df.count()
    rejected_count  = initial_count - validated_count
    rejection_rate  = (rejected_count / initial_count) * 100

    print(f"✅ Initial records  : {initial_count:,}")
    print(f"✅ Valid records    : {validated_count:,}")
    print(f"❌ Rejected records : {rejected_count:,}")
    print(f"📊 Rejection rate  : {rejection_rate:.2f}%")

    return validated_df


# Apply validation
validated_df = validate_silver_data(silver_df)

# COMMAND ----------

# ============================================
# Write Silver Delta table
# ============================================

print("Writing Silver table...")

validated_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("year", "month") \
    .saveAsTable("nyc_tlc.silver.hvfhv")

print("✅ Silver table created!")

# COMMAND ----------

# ============================================
# Silver Layer — Zone Lookup
# ============================================

from pyspark.sql.functions import trim

zone_bronze_df = spark.read.table("nyc_tlc.bronze.zone_lookup")

# Quality checks
total      = zone_bronze_df.count()
null_ids   = zone_bronze_df.filter(col("LocationID").isNull()).count()
duplicates = total - zone_bronze_df.dropDuplicates(["LocationID"]).count()

print(f"Total zones      : {total}")
print(f"Null LocationIDs : {null_ids}")
print(f"Duplicate IDs    : {duplicates}")

# Clean and standardize
zone_silver_df = zone_bronze_df.select(
    col("LocationID").cast("integer"),
    trim(col("Borough")).alias("borough"),
    trim(col("Zone")).alias("zone"),
    trim(col("service_zone")).alias("service_zone")
)

zone_silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("nyc_tlc.silver.zone_lookup")

print(f"✅ Silver zone_lookup created!")

# COMMAND ----------

# ============================================
# Optimize Silver table
# ============================================

print("Optimizing Silver table...")
spark.sql("""
    OPTIMIZE nyc_tlc.silver.hvfhv
    ZORDER BY (trip_date, PULocationID)
""")
print("✅ Optimization complete!")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     year,
# MAGIC     month,
# MAGIC     COUNT(*) as records
# MAGIC FROM nyc_tlc.silver.hvfhv
# MAGIC GROUP BY year, month
# MAGIC ORDER BY year, month;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) as total_records
# MAGIC FROM nyc_tlc.silver.hvfhv;
