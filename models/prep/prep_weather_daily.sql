

WITH daily_data AS (
    SELECT * 
    FROM {{ref('staging_weather_daily')}}
),
add_features AS (
    SELECT *
		, date_part('day',date) AS date_day 		-- number of the day of month
		, date_part('month',date) AS date_month 	-- number of the month of year
		, date_part('year',date) AS date_year 		-- number of year
		, date_part('week',date) AS cw 			-- number of the week of year
		, TO_CHAR(date, 'FMmonth') AS month_name 	-- name of the month
		, TO_CHAR(date, 'FMday') AS weekday 		-- name of the weekday
    FROM daily_data 
),
add_more_features AS (
    SELECT *
		, (CASE 
			WHEN month_name in ('january', 'december', 'fabruary') THEN 'winter'
			WHEN month_name IN ('march', 'april', 'may') THEN 'spring'
            WHEN month_name IN ('june', 'july', 'august') THEN 'summer'
            WHEN month_name IN ('september', 'october', 'november') THEN 'autumn'
		END) AS season
    FROM add_features
)
SELECT *
FROM add_more_features
ORDER BY date