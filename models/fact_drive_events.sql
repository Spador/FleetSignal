-- fact_drive_events: one row per telemetry event, the grain of the whole model.
-- Carries the measures and flags, the full-resolution event ts, and surrogate foreign keys
-- into the four dimensions.
--
-- miles_segment is derived per event: current speed times the time gap since this vehicle's
-- previous event. speed is mph and the gap is converted to hours, so the product is miles.
-- The first event for a vehicle has no previous event, so its segment is 0.

CREATE TABLE fact_drive_events AS
WITH events AS (
    SELECT
        *,
        COALESCE(
            speed_mph
            * (epoch(ts) - epoch(LAG(ts) OVER (PARTITION BY vehicle_id ORDER BY ts)))
            / 3600.0,
            0
        ) AS miles_segment
    FROM silver_events
)
SELECT
    e.event_id,
    e.ts,
    v.vehicle_key,
    t.time_key,
    w.weather_key,
    l.location_key,
    e.speed_mph,
    e.accel,
    e.autopilot_engaged,
    e.disengagement,
    e.hard_brake,
    e.miles_segment
FROM events e
JOIN dim_vehicle  v ON v.vehicle_id = e.vehicle_id AND v.is_current
JOIN dim_time     t ON t.hour_ts    = e.ts_hour
JOIN dim_weather  w ON w.condition  = e.weather
JOIN dim_location l ON l.lat_bucket = e.lat_bucket AND l.lon_bucket = e.lon_bucket;
