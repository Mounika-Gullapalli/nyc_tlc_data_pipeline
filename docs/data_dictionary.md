# Data Dictionary — NYC TLC HVFHV Pipeline

## Source Data
- **Dataset:** High Volume For-Hire Vehicle (HVFHV) Trip Records
- **Source:** NYC Taxi and Limousine Commission
- **URL:** https://d37ci6vzurychx.cloudfront.net/trip-data/
- **Format:** Parquet
- **Frequency:** Monthly (2 month publication delay)
- **Period:** January 2024 - April 2026

---

## Bronze Layer — nyc_tlc.bronze.hvfhv
Raw data as-is from NYC TLC website. No transformations applied.

| Column | Type | Description |
|--------|------|-------------|
| hvfhs_license_num | STRING | HVFHV license number (HV0002-HV0005) |
| dispatching_base_num | STRING | Base that dispatched the trip |
| originating_base_num | STRING | Base that originated the trip |
| request_datetime | TIMESTAMP | When trip was requested |
| on_scene_datetime | TIMESTAMP | When driver arrived at pickup |
| pickup_datetime | TIMESTAMP | When passenger was picked up |
| dropoff_datetime | TIMESTAMP | When passenger was dropped off |
| PULocationID | INTEGER | Pickup zone ID (1-263) |
| DOLocationID | INTEGER | Dropoff zone ID (1-263) |
| trip_miles | DOUBLE | Distance in miles |
| trip_time | INTEGER | Duration in seconds |
| base_passenger_fare | DOUBLE | Base fare amount ($) |
| tolls | DOUBLE | Toll charges ($) |
| bcf | DOUBLE | Black car fund fee ($) |
| sales_tax | DOUBLE | Sales tax ($) |
| congestion_surcharge | DOUBLE | Congestion pricing fee ($) |
| airport_fee | DOUBLE | Airport surcharge ($) |
| tips | DOUBLE | Tip amount ($) |
| driver_pay | DOUBLE | Driver earnings ($) |
| shared_request_flag | STRING | Y/N shared ride requested |
| shared_match_flag | STRING | Y/N shared ride matched |
| access_a_ride_flag | STRING | Y/N accessibility ride |
| wav_request_flag | STRING | Y/N wheelchair vehicle requested |
| wav_match_flag | STRING | Y/N wheelchair vehicle matched |
| year | INTEGER | Partition column (from file path) |
| month | INTEGER | Partition column (from file path) |

---

## Bronze Layer — nyc_tlc.bronze.zone_lookup
Raw zone reference data from NYC TLC.

| Column | Type | Description |
|--------|------|-------------|
| LocationID | INTEGER | Zone ID (1-263) |
| Borough | STRING | NYC borough name |
| Zone | STRING | Zone name |
| service_zone | STRING | Service area type |

---

## Silver Layer — nyc_tlc.silver.hvfhv
Cleaned and transformed data with derived fields.

| Column | Type | Description | Transformation |
|--------|------|-------------|----------------|
| hvfhs_license_num | STRING | License number | Passthrough |
| dispatching_base_num | STRING | Dispatching base | Passthrough |
| originating_base_num | STRING | Originating base | Passthrough |
| request_datetime | TIMESTAMP | Request time | Cast to timestamp |
| on_scene_datetime | TIMESTAMP | On scene time | Cast to timestamp |
| pickup_datetime | TIMESTAMP | Pickup time | Cast to timestamp |
| dropoff_datetime | TIMESTAMP | Dropoff time | Cast to timestamp |
| PULocationID | INTEGER | Pickup zone | Cast to integer |
| DOLocationID | INTEGER | Dropoff zone | Cast to integer |
| trip_miles | DECIMAL(10,2) | Trip distance | Null → 0.0 |
| trip_time_seconds | INTEGER | Duration seconds | Cast to integer |
| base_passenger_fare | DECIMAL(10,2) | Base fare | Null → 0.0 |
| tolls | DECIMAL(10,2) | Tolls | Null → 0.0 |
| bcf | DECIMAL(10,2) | Black car fund | Null → 0.0 |
| sales_tax | DECIMAL(10,2) | Sales tax | Null → 0.0 |
| congestion_surcharge | DECIMAL(10,2) | Congestion fee | Null → 0.0 |
| airport_fee | DECIMAL(10,2) | Airport fee | Null → 0.0 |
| tips | DECIMAL(10,2) | Tips | Null → 0.0 |
| driver_pay | DECIMAL(10,2) | Driver pay | Null → 0.0 |
| shared_request_flag | BOOLEAN | Shared requested | Y→True, N→False |
| shared_match_flag | BOOLEAN | Shared matched | Y→True, N→False |
| access_a_ride_flag | BOOLEAN | Accessibility | Y→True, N→False |
| wav_request_flag | BOOLEAN | WAV requested | Y→True, N→False |
| wav_match_flag | BOOLEAN | WAV matched | Y→True, N→False |
| trip_date | DATE | Trip date | From pickup_datetime |
| pickup_hour | INTEGER | Hour of pickup | From pickup_datetime |
| day_of_week | STRING | Day name | From pickup_datetime |
| is_weekend | BOOLEAN | Weekend flag | Sat/Sun = True |
| trip_duration_minutes | DECIMAL(10,2) | Duration minutes | trip_time / 60 |
| total_fare | DECIMAL(10,2) | Total fare | Sum of all charges |
| total_amount | DECIMAL(10,2) | Total with tips | total_fare + tips |
| avg_speed_mph | DECIMAL(10,2) | Average speed | miles / hours |
| is_airport_trip | BOOLEAN | Airport flag | airport_fee > 0 |
| year | INTEGER | Partition | From pickup_datetime |
| month | INTEGER | Partition | From pickup_datetime |
| day | INTEGER | Day of month | From pickup_datetime |
| processing_timestamp | TIMESTAMP | When processed | current_timestamp() |

### Validation Rules Applied:
| Rule | Description |
|------|-------------|
| Not null check | pickup_datetime, dropoff_datetime, PULocationID, DOLocationID |
| Range check | trip_miles between 0 and 500 |
| Range check | base_passenger_fare between 0 and 10,000 |
| Logic check | dropoff_datetime > pickup_datetime |
| Logic check | pickup_datetime >= request_datetime |
| Value check | hvfhs_license_num IN (HV0002, HV0003, HV0004, HV0005) |

---

## Silver Layer — nyc_tlc.silver.zone_lookup
Cleaned zone reference data.

| Column | Type | Description | Transformation |
|--------|------|-------------|----------------|
| LocationID | INTEGER | Zone ID | Cast to integer |
| borough | STRING | Borough name | Trimmed whitespace |
| zone | STRING | Zone name | Trimmed whitespace |
| service_zone | STRING | Service type | Trimmed whitespace |

---

## Gold Layer — nyc_tlc.gold.daily_borough
**Grain:** One row per day × pickup_borough × dropoff_borough

| Column | Type | Description |
|--------|------|-------------|
| trip_date | DATE | Date of trips |
| day_of_week | STRING | Day name (Monday-Sunday) |
| is_weekend | BOOLEAN | True if Saturday or Sunday |
| pickup_borough | STRING | Borough where passenger was picked up |
| dropoff_borough | STRING | Borough where passenger was dropped off |
| trip_count | LONG | Number of trips |
| total_revenue | DECIMAL(12,2) | Total revenue including tips ($) |
| total_fare | DECIMAL(12,2) | Total fare excluding tips ($) |
| total_tips | DECIMAL(12,2) | Total tips ($) |
| total_driver_pay | DECIMAL(12,2) | Total driver earnings ($) |
| total_distance | DECIMAL(12,2) | Total miles traveled |
| avg_trip_distance | DECIMAL(10,2) | Average miles per trip |
| avg_trip_duration | DECIMAL(10,2) | Average minutes per trip |
| avg_fare_per_trip | DECIMAL(10,2) | Average fare per trip ($) |
| year | INTEGER | Partition column |
| month | INTEGER | Partition column |

---

## Gold Layer — nyc_tlc.gold.daily_zone
**Grain:** One row per day × pickup_zone × dropoff_zone × airport_flag

| Column | Type | Description |
|--------|------|-------------|
| trip_date | DATE | Date of trips |
| PULocationID | INTEGER | Pickup zone ID |
| DOLocationID | INTEGER | Dropoff zone ID |
| is_airport_trip | BOOLEAN | True if airport fee > 0 |
| trip_count | LONG | Number of trips |
| total_revenue | DECIMAL(12,2) | Total revenue ($) |
| avg_trip_distance | DECIMAL(10,2) | Average miles |
| avg_trip_duration | DECIMAL(10,2) | Average minutes |
| avg_fare_per_trip | DECIMAL(10,2) | Average fare ($) |
| total_driver_pay | DECIMAL(12,2) | Total driver pay ($) |
| pickup_zone | STRING | Pickup zone name |
| pickup_borough | STRING | Pickup borough |
| pickup_service_zone | STRING | Pickup service zone type |
| dropoff_zone | STRING | Dropoff zone name |
| dropoff_borough | STRING | Dropoff borough |
| dropoff_service_zone | STRING | Dropoff service zone type |
| year | INTEGER | Partition column |
| month | INTEGER | Partition column |
| day | INTEGER | Partition column |

---

## Gold Layer — nyc_tlc.gold.daily_service
**Grain:** One row per day × shared_flag × wav_flag

| Column | Type | Description |
|--------|------|-------------|
| trip_date | DATE | Date of trips |
| day_of_week | STRING | Day name |
| is_weekend | BOOLEAN | Weekend flag |
| shared_request_flag | BOOLEAN | Shared ride requested |
| wav_request_flag | BOOLEAN | Wheelchair vehicle requested |
| trip_count | LONG | Number of trips |
| total_revenue | DECIMAL(12,2) | Total revenue ($) |
| avg_fare_per_trip | DECIMAL(10,2) | Average fare ($) |
| shared_matched_count | LONG | Shared rides successfully matched |
| wav_matched_count | LONG | WAV requests successfully matched |
| avg_trip_distance | DECIMAL(10,2) | Average miles |
| avg_trip_duration | DECIMAL(10,2) | Average minutes |
| year | INTEGER | Partition column |
| month | INTEGER | Partition column |

---

## HVFHV License Numbers

| License | Company |
|---------|---------|
| HV0002 | Juno |
| HV0003 | Uber |
| HV0004 | Via |
| HV0005 | Lyft |

---

## NYC Boroughs

| Borough | Description |
|---------|-------------|
| Manhattan | Most dense borough, highest revenue |
| Brooklyn | Largest by population |
| Queens | Home to JFK and LaGuardia airports |
| Bronx | North of Manhattan |
| Staten Island | Least populated borough |
| EWR | Newark Airport (New Jersey) |
