"""Run the transform pipeline: bronze -> silver -> gold star schema.

The transform logic lives in the .sql files under models/. This runner only reads bronze,
runs each SQL file against an in-memory DuckDB, and writes each result table to parquet.

Run:
    python3 pipeline.py
"""

import glob
import os
import shutil
import time

import duckdb


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

BRONZE_GLOB = "data/bronze/*/*.parquet"   # one level of date= partitions
SILVER_DIR = "data/silver"
GOLD_DIR = "data/gold"
MODELS_DIR = "models"

# Stages run in this order. Each is (sql file, table it creates, where to write it).
# dims come before the fact, because the fact joins to them.
STAGES = [
    ("silver.sql",            "silver_events",     SILVER_DIR),
    ("dim_weather.sql",       "dim_weather",       GOLD_DIR),
    ("dim_time.sql",          "dim_time",          GOLD_DIR),
    ("dim_location.sql",      "dim_location",      GOLD_DIR),
    ("dim_vehicle.sql",       "dim_vehicle",       GOLD_DIR),
    ("fact_drive_events.sql", "fact_drive_events", GOLD_DIR),
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def reset_dir(path):
    shutil.rmtree(path, ignore_errors=True)
    os.makedirs(path, exist_ok=True)


def run_stage(con, sql_file, table, out_dir):
    """Execute one model SQL file, write its table to parquet, return the row count."""
    sql = open(os.path.join(MODELS_DIR, sql_file)).read()
    con.execute(sql)
    out_path = os.path.join(out_dir, f"{table}.parquet")
    con.execute(f"COPY {table} TO '{out_path}' (FORMAT PARQUET)")
    return con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]


def main():
    if not glob.glob(BRONZE_GLOB):
        raise SystemExit(f"no bronze parquet found at {BRONZE_GLOB}. run generate.py first.")

    reset_dir(SILVER_DIR)
    reset_dir(GOLD_DIR)

    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'")   # data is UTC, so hour/day extraction is deterministic
    con.execute(f"CREATE VIEW bronze AS SELECT * FROM read_parquet('{BRONZE_GLOB}')")

    counts = {}
    for sql_file, table, out_dir in STAGES:
        start = time.time()
        counts[table] = run_stage(con, sql_file, table, out_dir)
        print(f"{table:<20} {counts[table]:>10,} rows  ({time.time() - start:.2f}s)")

    # Sanity: the fact is built from silver via inner joins, so it must not lose rows.
    if counts["fact_drive_events"] != counts["silver_events"]:
        print(f"WARNING: fact rows {counts['fact_drive_events']:,} != "
              f"silver rows {counts['silver_events']:,}, a join dropped events")
    else:
        print(f"ok: fact rows match silver rows ({counts['silver_events']:,})")


if __name__ == "__main__":
    main()
