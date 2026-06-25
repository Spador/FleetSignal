# FleetSignal

![pipeline](https://github.com/Spador/FleetSignal/actions/workflows/pipeline.yml/badge.svg)

A miniature fleet telemetry pipeline that mines synthetic Self-Driving data, computes a safety metric, and surfaces interesting driving scenarios for training and evaluation.

This is a self-contained build that mirrors the real loop a fleet data team runs: source high volume telemetry, move it through a layered pipeline, turn it into a metric that measures autonomy performance, and flag scenarios worth labeling.

## Why this exists

Most data projects stop at a dashboard. This one models the full data lifecycle end to end: sourcing, ingestion, transformation, analytics, scenario mining, and serving. The headline metric is miles per disengagement, the same class of number that shows up in a Vehicle Safety Report.

## Architecture

```
 Synthetic         Bronze            Silver             Gold              Serve
 generator   -->   raw events  -->   cleaned/deduped -->  aggregates  -->  dashboard
 (Python)          (parquet)         (SQL/dbt)          (star schema)     + scenario feed
                                                                          + health checks
```

- Bronze: raw telemetry as written, no changes
- Silver: typed, deduped, validated
- Gold: star schema with fact and dimension tables, ready for fast queries

## Data model

Telemetry event fields:

| field | meaning |
|---|---|
| vehicle_id | which car |
| ts | event timestamp |
| speed_mph | instantaneous speed |
| accel | longitudinal acceleration |
| lat, lon | location |
| autopilot_engaged | bool, autonomy active or manual |
| disengagement | bool, autonomy handed control back |
| hard_brake | bool, braking past a threshold |
| weather | clear, rain, snow, fog |

Gold star schema:

- fact_drive_events (one row per event, foreign keys to dims)
- dim_vehicle, dim_time, dim_weather, dim_location_bucket

## The headline metric

Miles per disengagement, plus interventions per 1000 miles.

```
miles_per_disengagement = total_autopilot_miles / count(disengagements)
intervention_rate_1k    = count(disengagements) / total_autopilot_miles * 1000
```

Sliced by:

- autopilot vs manual
- weather
- time of day

This is the number that drives decisions. One metric done deeply.

## Scenario mining

A query layer that ranks events worth pulling into a training or eval set:

- disengagements clustered near the same location
- hard braking events in rain or snow
- high speed events with autopilot engaged in poor weather

Output is a ranked candidate dataset, written to disk, ready to hand to an AI engineering partner.

## Dashboard

One interactive view:

- map of flagged scenarios
- safety metric trends over time, with the autopilot vs manual split
- filters for weather and time window

## Health and reliability

A check job that runs after each pipeline pass:

- row counts per stage, bronze to gold
- freshness lag, how old is the newest event
- an alert when inflow drops below an expected floor

This is the monitoring and alerting story, scaled down but real.

## Scale notes

State the real numbers from your own run here. Example shape:

- generator produces 5,000,000 events across 3,000 vehicles
- bronze write time, silver transform time, gold build time
- gold query latency for the metric and for scenario mining

This section is the proof of "ETLs at scale" without needing a real fleet.

## Stack

- Python for the generator and transforms
- DuckDB for the SQL warehouse layer
- dbt for silver to gold, optional but signals ETL discipline
- Streamlit for the dashboard and map
- cron or a small Airflow DAG for orchestration

## Run it

```
python generate.py --vehicles 3000 --events 5000000
python pipeline.py            # bronze -> silver -> gold
python health_check.py        # counts, freshness, alert
streamlit run app.py          # dashboard
```

## Repo layout

```
fleetsignal/
  generate.py
  pipeline.py
  health_check.py
  app.py
  models/            # dbt or raw SQL transforms
  data/
    bronze/
    silver/
    gold/
  scenarios/         # ranked candidate datasets
  README.md
```

## What this demonstrates

- Python and SQL at volume
- full data lifecycle ownership, source to serve
- a metric that measures Self-Driving performance, defined and built
- scenario sourcing for training and eval
- monitoring, alerting, and orchestration
- interactive visualization that turns raw events into a decision
