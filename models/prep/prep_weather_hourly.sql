

WITH hourly_data AS (
    SELECT * 
    FROM {{ref('staging_weather_hourly')}}
),
add_features AS (
    SELECT *,
        timestamp::DATE AS date,                        -- Extract only the date (without time)
        timestamp::TIME AS time,                        -- Extract only the time (HH:MI:SS)
        TO_CHAR(timestamp, 'HH24:MI') AS hour,          -- Extract time (HH:MI) as TEXT
        TO_CHAR(timestamp, 'FMMonth') AS month_name,    -- Extract full month name as TEXT
        TO_CHAR(timestamp, 'Day') AS weekday,           -- Extract weekday name as TEXT        
        DATE_PART('day', timestamp) AS date_day,        -- Extract day of the month
        DATE_PART('month', timestamp) AS date_month,    -- Extract month number
        DATE_PART('year', timestamp) AS date_year,      -- Extract year
        TO_CHAR(timestamp, 'IYYY-IW') AS cw             -- Extract calendar week (ISO format)
    FROM hourly_data
),
add_more_features AS (
    SELECT *,
        (CASE 
            WHEN time BETWEEN '00:00:00' AND '05:59:59' THEN 'night'
            WHEN time BETWEEN '06:00:00' AND '11:59:59' THEN 'morning'
            WHEN time BETWEEN '12:00:00' AND '17:59:59' THEN 'afternoon'
            WHEN time BETWEEN '18:00:00' AND '23:59:59' THEN 'evening'
        END) AS day_part
    FROM add_features
)
SELECT * FROM add_more_features;