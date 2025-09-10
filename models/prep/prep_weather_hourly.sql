WITH hourly_data AS (
    SELECT * 
    FROM {{ref('staging_weather_hourly')}}
),
add_features AS (
    SELECT *
		, timestamp::DATE AS date               -- only date (hours:minutes:seconds) as DATE data type
		, timestamp AS time                           -- only time (hours:minutes:seconds) as TIME data type
        , TO_CHAR(timestamp,'HH24:MI') as hour  -- time (hours:minutes) as TEXT data type
        , TO_CHAR(timestamp, 'FMmonth') AS month_name   -- month name as a TEXT
        , to_char(timestamp, 'Day' ) AS weekday        -- weekday name as TEXT        
        , DATE_PART('day', timestamp) AS date_day
		, DATE_PART('month', timestamp) AS date_month
		, DATE_PART('year', timestamp) AS date_year
		, DATE_PART('week', timestamp) AS cw
    FROM hourly_data
)
,
add_more_features AS (
    SELECT *
		,(CASE 
			WHEN hour BETWEEN '20:00:00' AND '23:59:59'  THEN 'night'
			WHEN hour BETWEEN '23:59:59' AND '05:59:59' THEN 'night'
			WHEN hour BETWEEN '06:00:00' AND '15:59:59' THEN 'day'
			ELSE 'evening'
		END) AS day_part
    FROM add_features
)
SELECT *
FROM add_more_features