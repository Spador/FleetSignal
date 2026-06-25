-- Silver: the clean, typed, deduplicated event table.
-- Reads the `bronze` view (registered by pipeline.py) and produces one row per event.
--
-- Config: GRID_DECIMALS = 3 controls the location grid (~110 m). The hour bucket and the
-- lat/lon buckets are computed here, once, so dim_time, dim_location and the fact all agree.

CREATE TABLE silver_events AS
WITH deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY ts) AS row_in_event
    FROM bronze
)
SELECT
    CAST(event_id AS BIGINT)            AS event_id,
    CAST(vehicle_id AS VARCHAR)         AS vehicle_id,
    ts,
    CAST(speed_mph AS DOUBLE)           AS speed_mph,
    CAST(accel AS DOUBLE)               AS accel,
    CAST(lat AS DOUBLE)                 AS lat,
    CAST(lon AS DOUBLE)                 AS lon,
    CAST(autopilot_engaged AS BOOLEAN)  AS autopilot_engaged,
    CAST(disengagement AS BOOLEAN)      AS disengagement,
    CAST(hard_brake AS BOOLEAN)         AS hard_brake,
    CAST(weather AS VARCHAR)            AS weather,
    date_trunc('hour', ts)              AS ts_hour,
    round(lat, 3)                       AS lat_bucket,
    round(lon, 3)                       AS lon_bucket
FROM deduped
WHERE row_in_event = 1;
