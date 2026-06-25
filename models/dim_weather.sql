-- dim_weather: one row per weather condition, with a surrogate key.

CREATE TABLE dim_weather AS
SELECT
    ROW_NUMBER() OVER (ORDER BY condition) AS weather_key,
    condition
FROM (
    SELECT DISTINCT weather AS condition
    FROM silver_events
);
