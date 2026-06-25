-- dim_time: one row per hour. The exact event timestamp stays on the fact table; this
-- dimension exists only to slice by hour, day, and time_of_day.

CREATE TABLE dim_time AS
SELECT
    ROW_NUMBER() OVER (ORDER BY hour_ts) AS time_key,
    hour_ts,
    hour(hour_ts)        AS hour,
    CAST(hour_ts AS DATE) AS day,
    CASE
        WHEN hour(hour_ts) < 6  THEN 'night'
        WHEN hour(hour_ts) < 12 THEN 'morning'
        WHEN hour(hour_ts) < 18 THEN 'afternoon'
        ELSE 'evening'
    END AS time_of_day
FROM (
    SELECT DISTINCT ts_hour AS hour_ts
    FROM silver_events
);
