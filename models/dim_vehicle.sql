-- dim_vehicle: slowly changing dimension, type 2.
-- A vehicle attribute like model could change over time, so the table carries
-- valid_from / valid_to / is_current to preserve history. The generator does not change
-- attributes tonight, so each vehicle has exactly one current row.
--
-- model is not in the bronze schema (the 11 telemetry fields), so we derive it
-- deterministically from the vehicle number. Same vehicle, same model, every run.

CREATE TABLE dim_vehicle AS
SELECT
    ROW_NUMBER() OVER (ORDER BY vehicle_id) AS vehicle_key,
    vehicle_id,
    CASE CAST(SUBSTR(vehicle_id, 4) AS INTEGER) % 4
        WHEN 0 THEN 'Model S'
        WHEN 1 THEN 'Model 3'
        WHEN 2 THEN 'Model X'
        ELSE        'Model Y'
    END AS model,
    first_seen              AS valid_from,
    CAST(NULL AS TIMESTAMP WITH TIME ZONE) AS valid_to,
    TRUE                    AS is_current
FROM (
    SELECT vehicle_id, MIN(ts) AS first_seen
    FROM silver_events
    GROUP BY vehicle_id
);
