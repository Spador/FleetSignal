# FleetSignal

![pipeline](https://github.com/Spador/FleetSignal/actions/workflows/pipeline.yml/badge.svg)

A small fleet telemetry pipeline. It generates synthetic Self-Driving events, moves them through a
layered warehouse, computes a safety metric, mines interesting scenarios, and guards the whole flow
with data quality gates and statistical anomaly detection. One command runs all of it.

The data is synthetic on purpose. The role is about building the pipeline, not finding a dataset.
Synthetic data lets us control volume to prove scale, and inject faults to prove the failure
detection works.

## Architecture

```
generate.py          bronze            silver              gold             serve
(pure Python)  -->    raw events  -->   clean, deduped  --> star schema  --> app.py dashboard
                      parquet           one wide table      fact + dims      + scenario files
                      by date                                                + quality report

         quality gates between the stages   |   anomaly detection at the end
```

Three layers, the standard medallion pattern. Each layer has one job, so a failure is easy to
locate. Bronze proves what arrived, silver proves what is clean, gold proves what is modeled.

## The metric

Miles per disengagement, the number a Vehicle Safety Report leads with. Plus the intervention rate
per 1,000 autopilot miles.

```
miles_per_disengagement  = total_autopilot_miles / disengagement_count
intervention_rate_per_1k = disengagement_count / total_autopilot_miles * 1000
```

Computed in `metrics.sql`, sliced by autopilot vs manual, weather, and time of day. Both ratios are
defined over autopilot miles only, since a disengagement cannot happen in manual mode. One metric
done deeply beats five done shallow.

## Scenario mining

`scenarios.sql` writes three ranked candidate datasets to `scenarios/`, each framed as a handoff to
an AI engineering partner:

- disengagement clusters, the recurring trouble spots by location
- hard brakes in rain or snow, ranked by how hard
- high speed autopilot in poor weather, ranked by speed

## Failure detection

`quality.py`, two tiers, run inside the pipeline and able to halt it.

- Tier 1, deterministic gates: schema, nulls, ranges, uniqueness, referential integrity. Pass or
  stop. The Tier 1 gate sits between silver and gold, so bad data halts before it is modeled.
- Tier 2, statistical anomaly detection against a rolling baseline: volume drop, freshness lag,
  distribution drift, vehicle dropout. A warn stays green, a fail goes red and exits nonzero.

The generator can inject four faults (`--inject-anomaly`), and each one is caught by a specific
check. Simple statistics, not a model, because it is explainable line by line and needs no training
data.

## Dashboard

`streamlit run app.py`. Dark theme. Three metric cards, a pydeck hotspot map of flagged scenarios,
and a safety trend of interventions per 1k autopilot miles by hour.

## Run it

```
pip install -r requirements.txt

python3 orchestrate.py --vehicles 1000 --events 1000000   # generate -> ... -> anomaly check
streamlit run app.py                                       # the dashboard
```

`orchestrate.py` is the one command. It runs every stage in order, times each one, retries once on
error, and halts with a nonzero exit if a gate fails. Run it a few times to build the anomaly
baseline, then inject a fault to watch it go red:

```
python3 orchestrate.py --inject-anomaly spike_disengagements
```

## Scale notes

Real numbers from this build, on a laptop. Generation is pure standard-library Python on purpose,
so the code reads clearly. That makes it the bottleneck. The DuckDB transform layer stays under two
seconds even at five million events.

| events | vehicles | generate | silver | gold | metrics + scenarios | total |
|---|---|---|---|---|---|---|
| 1,000,000 | 1,000 | 8.6s | 0.4s | 0.3s | 0.1s | 10.2s |
| 5,000,000 | 2,000 | 64.2s | 1.1s | 0.8s | 0.2s | 67.2s |

At five million events: silver and fact are 5,000,000 rows each, dim_location 21,091, dim_vehicle
2,000, dim_time 72, dim_weather 4. The whole warehouse runs on parquet with DuckDB, no server to
host.

## Stack

- Python for generation, transforms, orchestration
- DuckDB as the warehouse, SQL on parquet with zero setup
- Parquet for storage at each layer
- Plain SQL files for the transforms and the metric
- Streamlit and pydeck for the dashboard
- GitHub Actions for scheduled automation
- pandas and pyarrow only to write parquet at the bronze layer

## Repo layout

```
fleetsignal/
  generate.py          # synthetic events -> bronze, with --inject-anomaly
  pipeline.py          # bronze -> silver -> gold
  quality.py           # two-tier failure detection
  orchestrate.py       # one command, all stages, gates wired in
  metrics.sql          # the safety metric
  scenarios.sql        # three ranked candidate datasets
  app.py               # the dashboard
  models/              # the silver and gold SQL transforms
  tests/               # pytest over the anomaly functions
  .github/workflows/   # the CI pipeline
  .streamlit/          # dark theme
```

## What this demonstrates

- Python and SQL at volume, a clean star schema with documented modeling decisions
- a safety metric that measures Self-Driving performance, defined and built
- scenario sourcing for training and evaluation
- two tiers of failure detection, with a demo that catches an injected fault
- one-command orchestration and unattended CI
- an interactive dashboard that turns raw events into a decision
