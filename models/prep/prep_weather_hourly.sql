WITH hourly_data AS (
    SELECT * 
    FROM {{ref('staging_weather_hourly')}}
),
add_features AS (
    SELECT *,
        timestamp::DATE AS date,                  -- Extract only date
        timestamp::TIME AS time,                  -- Extract only time
        TO_CHAR(timestamp, 'HH24:MI') AS hour,    -- Extract time as text (HH:MI)
        TO_CHAR(timestamp, 'FMMonth') AS month_name,  -- Extract full month name
        TRIM(TO_CHAR(timestamp, 'Day')) AS weekday,   -- Extract weekday name (trimmed)
        DATE_PART('day', timestamp) AS date_day,  -- Extract day of the month
        DATE_PART('month', timestamp) AS date_month,  -- Extract month number
        DATE_PART('year', timestamp) AS date_year,  -- Extract year
        TO_CHAR(timestamp, 'IYYY-IW') AS cw  -- Extract ISO calendar week
    FROM hourly_data
),
add_more_features AS (
    SELECT *,
        (CASE 
            WHEN time >= '22:00:00' OR time < '06:00:00' THEN 'night'  -- Fixed the midnight wrap issue
            WHEN time BETWEEN '06:00:00' AND '17:59:59' THEN 'day'
            WHEN time BETWEEN '18:00:00' AND '21:59:59' THEN 'evening'
        END) AS day_part
    FROM add_features
)

SELECT *
FROM add_more_features