"""Run the transform pipeline: bronze -> silver -> gold star schema.

The transform logic lives in the .sql files under models/. This runner only reads each
layer's parquet, runs each SQL file against an in-memory DuckDB, and writes the result.

Silver and gold are separate functions so the orchestrator can run a quality gate between
them. Each layer reads the previous layer's parquet, so they are independent steps.

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
SILVER_PARQUET = f"{SILVER_DIR}/silver_events.parquet"

# Each stage is (sql file, table it creates, where to write it).
SILVER_STAGE = ("silver.sql", "silver_events", SILVER_DIR)

# dims come before the fact, because the fact joins to them.
GOLD_STAGES = [
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


def connect():
    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'")   # data is UTC, so hour/day extraction is deterministic
    return con


def run_stage(con, sql_file, table, out_dir):
    """Execute one model SQL file, write its table to parquet, return the row count."""
    sql = open(os.path.join(MODELS_DIR, sql_file)).read()
    con.execute(sql)
    out_path = os.path.join(out_dir, f"{table}.parquet")
    con.execute(f"COPY {table} TO '{out_path}' (FORMAT PARQUET)")
    return con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]


# ---------------------------------------------------------------------------
# Layers
# ---------------------------------------------------------------------------

def build_silver():
    """Bronze -> silver: the clean, typed, deduplicated event table."""
    if not glob.glob(BRONZE_GLOB):
        raise SystemExit(f"no bronze parquet found at {BRONZE_GLOB}. run generate.py first.")
    reset_dir(SILVER_DIR)

    con = connect()
    con.execute(f"CREATE VIEW bronze AS SELECT * FROM read_parquet('{BRONZE_GLOB}')")
    sql_file, table, out_dir = SILVER_STAGE
    start = time.time()
    rows = run_stage(con, sql_file, table, out_dir)
    print(f"{table:<20} {rows:>10,} rows  ({time.time() - start:.2f}s)")
    con.close()
    return rows


def build_gold():
    """Silver -> gold: the star schema (four dims plus the fact)."""
    reset_dir(GOLD_DIR)

    con = connect()
    con.execute(f"CREATE VIEW silver_events AS SELECT * FROM read_parquet('{SILVER_PARQUET}')")
    counts = {}
    for sql_file, table, out_dir in GOLD_STAGES:
        start = time.time()
        counts[table] = run_stage(con, sql_file, table, out_dir)
        print(f"{table:<20} {counts[table]:>10,} rows  ({time.time() - start:.2f}s)")
    con.close()
    return counts


def main():
    silver_rows = build_silver()
    counts = build_gold()

    # Sanity: the fact is built from silver via inner joins, so it must not lose rows.
    if counts["fact_drive_events"] != silver_rows:
        print(f"WARNING: fact rows {counts['fact_drive_events']:,} != "
              f"silver rows {silver_rows:,}, a join dropped events")
    else:
        print(f"ok: fact rows match silver rows ({silver_rows:,})")


if __name__ == "__main__":
    main()
