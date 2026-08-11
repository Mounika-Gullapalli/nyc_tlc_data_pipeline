-- ============================================
-- NYC TLC Business Insights
-- Database: NYCTLC_DB | Schema: ANALYTICS
-- ============================================

use database NYCTLC_DB;
use schema ANALYTICS;
use warehouse NYCTLC_WH;


-- 1. Driver Earnings Optimization
-- When and where do drivers earn the most?
select
    day_of_week,
    pickup_borough,
    sum(total_driver_pay) as driver_earnings,
    sum(trip_count) as total_trips,
    round(sum(total_driver_pay)/sum(trip_count), 2) as earnings_per_trip
from DAILY_BOROUGH
group by day_of_week, pickup_borough
order by earnings_per_trip desc
limit 10;


-- 2. Market Growth Analysis
-- Is the ride sharing market growing month over month?
select
    year,
    month,
    sum(trip_count) as total_trips,
    sum(total_revenue) as total_revenue,
    lag(sum(trip_count)) over (order by year, month) as prev_month_trips,
    round(
        (sum(trip_count) - lag(sum(trip_count)) over (order by year, month))
        / lag(sum(trip_count)) over (order by year, month) * 100
    , 2) as growth_pct
from DAILY_BOROUGH
group by year, month
order by year, month;


-- 3. Underserved Areas
-- Which boroughs have highest demand but lowest supply?
select
    pickup_borough,
    sum(trip_count) as total_demand,
    avg(avg_trip_duration) as avg_duration_mins,
    sum(total_revenue) as total_revenue
from DAILY_BOROUGH
group by pickup_borough
order by total_demand desc;


-- 4. Airport Revenue Analysis
-- How much revenue comes from airport trips?
select
    is_airport_trip,
    sum(trip_count) as total_trips,
    sum(total_revenue) as total_revenue,
    round(sum(total_revenue) * 100.0 /
        sum(sum(total_revenue)) over (), 2) as revenue_pct,
    avg(avg_fare_per_trip) as avg_fare,
    avg(avg_trip_distance) as avg_distance_miles
from DAILY_ZONE
group by is_airport_trip;


-- 5. Shared Ride Business Case
-- Is shared ride matching improving over time?
select
    year,
    month,
    sum(case when shared_request_flag = 'true'
        then trip_count else 0 end) as shared_requests,
    sum(case when shared_request_flag = 'true'
        then shared_matched_count else 0 end) as shared_matched,
    round(
        sum(case when shared_request_flag = 'true'
            then shared_matched_count else 0 end) * 100.0 /
        nullif(sum(case when shared_request_flag = 'true'
            then trip_count else 0 end), 0)
    , 2) as match_rate_pct
from DAILY_SERVICE
group by year, month
order by year, month;


-- 6. Peak Revenue by Day of Week
-- Which day generates most revenue?
select
    day_of_week,
    sum(trip_count) as total_trips,
    sum(total_revenue) as total_revenue,
    avg(avg_fare_per_trip) as avg_fare,
    round(sum(total_revenue) * 100.0 /
        sum(sum(total_revenue)) over (), 2) as revenue_share_pct
from DAILY_BOROUGH
group by day_of_week
order by total_revenue desc;


-- 7. WAV Service Gap Analysis
-- Are wheelchair accessible vehicles meeting demand?
select
    year,
    month,
    sum(case when wav_request_flag = 'true'
        then trip_count else 0 end) as wav_requests,
    sum(case when wav_request_flag = 'true'
        then wav_matched_count else 0 end) as wav_matched,
    round(
        sum(case when wav_request_flag = 'true'
            then wav_matched_count else 0 end) * 100.0 /
        nullif(sum(case when wav_request_flag = 'true'
            then trip_count else 0 end), 0)
    , 2) as match_rate_pct
from DAILY_SERVICE
group by year, month
order by year, month;