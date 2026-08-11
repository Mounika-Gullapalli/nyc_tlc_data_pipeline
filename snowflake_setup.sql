-- NYC TLC data ingestion from Azure blob storage into Snowflake analytics tables
use database NYCTLC_DB;
use schema ANALYTICS;
use warehouse NYCTLC_WH;

-- storage integration
create or replace storage integration azure_nyc_tlc
    type = external_stage
    storage_provider = 'AZURE'
    enabled = true
    -- NOTE: Replace placeholders before running:
    -- <your_azure_tenant_id> → your Azure tenant ID
    -- Found in: Azure Portal → Microsoft Entra ID
    azure_tenant_id = <your_azure_tenant_id>
    storage_allowed_locations = ('azure://nyctlcdatahubextdl2026.blob.core.windows.net/nyc-tlc/gold/');

-- stage
create or replace stage azure_gold_stage
    url = 'azure://nyctlcdatahubextdl2026.blob.core.windows.net/nyc-tlc/gold/'
    storage_integration = azure_nyc_tlc
    file_format = (type = parquet);

list @azure_gold_stage;

-- daily_borough
create or replace table NYCTLC_DB.ANALYTICS.DAILY_BOROUGH (
    TRIP_DATE date,
    DAY_OF_WEEK varchar,
    IS_WEEKEND varchar,
    PICKUP_BOROUGH varchar,
    DROPOFF_BOROUGH varchar,
    TRIP_COUNT number,
    TOTAL_REVENUE number(12,2),
    TOTAL_FARE number(12,2),
    TOTAL_TIPS number(12,2),
    TOTAL_DRIVER_PAY number(12,2),
    TOTAL_DISTANCE number(12,2),
    AVG_TRIP_DISTANCE number(10,2),
    AVG_TRIP_DURATION number(10,2),
    AVG_FARE_PER_TRIP number(10,2),
    YEAR number,
    MONTH number
);

copy into NYCTLC_DB.ANALYTICS.DAILY_BOROUGH
from (
    select
        $1:trip_date::date,
        $1:day_of_week::varchar,
        $1:is_weekend::varchar,
        $1:pickup_borough::varchar,
        $1:dropoff_borough::varchar,
        $1:trip_count::number,
        $1:total_revenue::number(12,2),
        $1:total_fare::number(12,2),
        $1:total_tips::number(12,2),
        $1:total_driver_pay::number(12,2),
        $1:total_distance::number(12,2),
        $1:avg_trip_distance::number(10,2),
        $1:avg_trip_duration::number(10,2),
        $1:avg_fare_per_trip::number(10,2),
        $1:year::number,
        $1:month::number
    from @azure_gold_stage/daily_borough/
)
file_format = (type = parquet)
on_error = continue;

-- daily_zone
create or replace table NYCTLC_DB.ANALYTICS.DAILY_ZONE (
    TRIP_DATE date,
    PULOCATIONID number,
    DOLOCATIONID number,
    IS_AIRPORT_TRIP varchar(10),
    TRIP_COUNT number,
    TOTAL_REVENUE number(12,2),
    AVG_TRIP_DISTANCE number(10,2),
    AVG_TRIP_DURATION number(10,2),
    AVG_FARE_PER_TRIP number(10,2),
    TOTAL_DRIVER_PAY number(12,2),
    PICKUP_ZONE varchar(100),
    PICKUP_BOROUGH varchar(50),
    PICKUP_SERVICE_ZONE varchar(50),
    DROPOFF_ZONE varchar(100),
    DROPOFF_BOROUGH varchar(50),
    DROPOFF_SERVICE_ZONE varchar(50),
    YEAR number,
    MONTH number,
    DAY number
);

copy into NYCTLC_DB.ANALYTICS.DAILY_ZONE
from @azure_gold_stage/daily_zone/
file_format = (type = parquet snappy_compression = true)
match_by_column_name = case_insensitive
on_error = continue;

-- daily_service
create or replace table NYCTLC_DB.ANALYTICS.DAILY_SERVICE (
    TRIP_DATE date,
    DAY_OF_WEEK varchar(20),
    IS_WEEKEND varchar(10),
    SHARED_REQUEST_FLAG varchar(10),
    WAV_REQUEST_FLAG varchar(10),
    TRIP_COUNT number,
    TOTAL_REVENUE number(12,2),
    AVG_FARE_PER_TRIP number(10,2),
    SHARED_MATCHED_COUNT number,
    WAV_MATCHED_COUNT number,
    AVG_TRIP_DISTANCE number(10,2),
    AVG_TRIP_DURATION number(10,2),
    YEAR number,
    MONTH number
);

copy into NYCTLC_DB.ANALYTICS.DAILY_SERVICE
from @azure_gold_stage/daily_service/
file_format = (type = parquet snappy_compression = true)
match_by_column_name = case_insensitive
on_error = continue;

-- verify
select 'DAILY_BOROUGH' as table_name, count(*) as records from DAILY_BOROUGH
union all
select 'DAILY_ZONE', count(*) from DAILY_ZONE
union all
select 'DAILY_SERVICE', count(*) from DAILY_SERVICE;