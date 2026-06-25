-- scenarios.sql
-- Three ranked candidate datasets for training or evaluation, mined from gold.
-- Each output is an explicitly ranked list written to scenarios/ as parquet, framed as a
-- dataset to hand to an AI engineering partner. event_id is carried so a consumer can join
-- back to silver or bronze for raw coordinates.
--
-- Config:
--   HIGH_SPEED_THRESHOLD   = 65 mph   (see scenario 3)
--   hard-brake bad weather = rain or snow            (scenario 2)
--   high-speed poor weather = anything except clear  (scenario 3)

-- 1. Disengagement clusters: recurring trouble spots, grouped by location grid cell.
COPY (
    SELECT
        ROW_NUMBER() OVER (ORDER BY count(*) DESC) AS rank,
        l.lat_bucket,
        l.lon_bucket,
        count(*) AS disengagement_count
    FROM read_parquet('data/gold/fact_drive_events.parquet') f
    JOIN read_parquet('data/gold/dim_location.parquet') l USING (location_key)
    WHERE f.disengagement
    GROUP BY l.lat_bucket, l.lon_bucket
    ORDER BY disengagement_count DESC
) TO 'scenarios/disengagement_clusters.parquet' (FORMAT PARQUET);

-- 2. Hard brakes in bad weather, ranked by severity of deceleration (most negative first).
COPY (
    SELECT
        ROW_NUMBER() OVER (ORDER BY f.accel ASC) AS rank,
        f.event_id,
        f.ts,
        v.vehicle_id,
        f.speed_mph,
        f.accel,
        w.condition AS weather,
        l.lat_bucket,
        l.lon_bucket
    FROM read_parquet('data/gold/fact_drive_events.parquet') f
    JOIN read_parquet('data/gold/dim_weather.parquet')  w USING (weather_key)
    JOIN read_parquet('data/gold/dim_vehicle.parquet')  v USING (vehicle_key)
    JOIN read_parquet('data/gold/dim_location.parquet') l USING (location_key)
    WHERE f.hard_brake AND w.condition IN ('rain', 'snow')
    ORDER BY f.accel ASC
) TO 'scenarios/hard_brakes_bad_weather.parquet' (FORMAT PARQUET);

-- 3. High speed autopilot in poor weather, ranked by speed (fastest first).
COPY (
    SELECT
        ROW_NUMBER() OVER (ORDER BY f.speed_mph DESC) AS rank,
        f.event_id,
        f.ts,
        v.vehicle_id,
        f.speed_mph,
        f.accel,
        w.condition AS weather,
        l.lat_bucket,
        l.lon_bucket
    FROM read_parquet('data/gold/fact_drive_events.parquet') f
    JOIN read_parquet('data/gold/dim_weather.parquet')  w USING (weather_key)
    JOIN read_parquet('data/gold/dim_vehicle.parquet')  v USING (vehicle_key)
    JOIN read_parquet('data/gold/dim_location.parquet') l USING (location_key)
    WHERE f.autopilot_engaged AND f.speed_mph > 65 AND w.condition <> 'clear'
    ORDER BY f.speed_mph DESC
) TO 'scenarios/highspeed_autopilot_poor_weather.parquet' (FORMAT PARQUET);
