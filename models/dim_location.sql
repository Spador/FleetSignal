-- dim_location: one row per grid cell. Raw lat/lon never repeats, so we group nearby
-- events by the rounded buckets computed in silver.

CREATE TABLE dim_location AS
SELECT
    ROW_NUMBER() OVER (ORDER BY lat_bucket, lon_bucket) AS location_key,
    lat_bucket,
    lon_bucket
FROM (
    SELECT DISTINCT lat_bucket, lon_bucket
    FROM silver_events
);
