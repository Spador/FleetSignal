-- metrics.sql
-- The one headline safety metric, computed over the gold star schema.
--
-- Definitions (spec section 7):
--   total_autopilot_miles    = sum of miles_segment where autopilot_engaged
--   disengagement_count      = count of events where disengagement
--   miles_per_disengagement  = total_autopilot_miles / disengagement_count
--   intervention_rate_per_1k = disengagement_count / total_autopilot_miles * 1000
--
-- The two ratios are autopilot-only by definition: a disengagement cannot happen in manual
-- mode, so manual rows carry null for both. nullif guards the zero-disengagement case so
-- the null is intentional, not a divide error.
--
-- Output: one tidy row per slice -> data/gold/metric_safety.parquet

COPY (
    WITH base AS (
        SELECT
            f.autopilot_engaged,
            w.condition AS weather,
            t.time_of_day,
            f.miles_segment,
            f.disengagement
        FROM read_parquet('data/gold/fact_drive_events.parquet') f
        JOIN read_parquet('data/gold/dim_weather.parquet') w USING (weather_key)
        JOIN read_parquet('data/gold/dim_time.parquet')    t USING (time_key)
    ),
    sliced AS (
        SELECT 'overall' AS slice_dimension, 'all' AS slice_value,
               sum(miles_segment) AS total_miles,
               sum(CASE WHEN autopilot_engaged THEN miles_segment ELSE 0 END) AS total_autopilot_miles,
               count(*) FILTER (WHERE disengagement) AS disengagement_count,
               count(*) AS event_count
        FROM base

        UNION ALL
        SELECT 'mode',
               CASE WHEN autopilot_engaged THEN 'autopilot' ELSE 'manual' END,
               sum(miles_segment),
               sum(CASE WHEN autopilot_engaged THEN miles_segment ELSE 0 END),
               count(*) FILTER (WHERE disengagement),
               count(*)
        FROM base
        GROUP BY 2

        UNION ALL
        SELECT 'weather', weather,
               sum(miles_segment),
               sum(CASE WHEN autopilot_engaged THEN miles_segment ELSE 0 END),
               count(*) FILTER (WHERE disengagement),
               count(*)
        FROM base
        GROUP BY 2

        UNION ALL
        SELECT 'time_of_day', time_of_day,
               sum(miles_segment),
               sum(CASE WHEN autopilot_engaged THEN miles_segment ELSE 0 END),
               count(*) FILTER (WHERE disengagement),
               count(*)
        FROM base
        GROUP BY 2
    )
    SELECT
        slice_dimension,
        slice_value,
        total_miles,
        total_autopilot_miles,
        disengagement_count,
        event_count,
        total_autopilot_miles / nullif(disengagement_count, 0)          AS miles_per_disengagement,
        disengagement_count / nullif(total_autopilot_miles, 0) * 1000   AS intervention_rate_per_1k
    FROM sliced
    ORDER BY slice_dimension, slice_value
) TO 'data/gold/metric_safety.parquet' (FORMAT PARQUET);
