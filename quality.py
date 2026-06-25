"""Failure detection, two tiers (spec section 9).

Tier 1 is deterministic hard gates: pass or stop. Tier 2 is statistical anomaly detection
against a rolling baseline in state/run_history.parquet, with a warn band and a fail band.

The script writes a JSON report to reports/, prints a colored summary, and exits nonzero on
any Tier 1 failure or any Tier 2 critical (fail) anomaly.

Run:
    python3 quality.py              # check, then append this run to the baseline
    python3 quality.py --no-append  # check against the baseline without recording this run
"""

import argparse
import json
import os
import sys
from datetime import datetime, timezone

import duckdb
import pandas as pd

# The gate checks against the exact bounds the generator used, so import them once.
from generate import (
    SPEED_MIN, SPEED_MAX, ACCEL_MIN, ACCEL_MAX,
    LAT_MIN, LAT_MAX, LON_MIN, LON_MAX,
)


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

SILVER_PARQUET = "data/silver/silver_events.parquet"
GOLD_DIR = "data/gold"
FACT = f"{GOLD_DIR}/fact_drive_events.parquet"
RUN_HISTORY = "state/run_history.parquet"
RUN_VEHICLES = "state/run_vehicles.parquet"   # vehicle ids of the last run, for set-diff dropout
REPORT = "reports/quality_report.json"

REQUIRED_FIELDS = ["event_id", "vehicle_id", "ts", "speed_mph"]

# The 11 telemetry fields and the DuckDB type each must have in silver.
EXPECTED_SCHEMA = {
    "event_id": "BIGINT",
    "vehicle_id": "VARCHAR",
    "ts": "TIMESTAMP WITH TIME ZONE",
    "speed_mph": "DOUBLE",
    "accel": "DOUBLE",
    "lat": "DOUBLE",
    "lon": "DOUBLE",
    "autopilot_engaged": "BOOLEAN",
    "disengagement": "BOOLEAN",
    "hard_brake": "BOOLEAN",
    "weather": "VARCHAR",
}

# Fact foreign keys and the dimension each must resolve to.
FACT_FOREIGN_KEYS = [
    ("vehicle_key", "dim_vehicle"),
    ("time_key", "dim_time"),
    ("weather_key", "dim_weather"),
    ("location_key", "dim_location"),
]

# Tier 2 thresholds. Named here, never inline. Two bands per check: warn stays green,
# fail goes red. The fail band is set so the four injected faults all turn the run red.
TRAILING_WINDOW = 10            # how many recent runs form the baseline
MIN_HISTORY = 3                 # need this many prior runs before stats are trustworthy

VOLUME_DROP_WARN_FRAC = 0.15    # row_count this far below the mean -> warn
VOLUME_DROP_FAIL_FRAC = 0.30    # this far below -> fail
VOLUME_DROP_WARN_Z = -1.5
VOLUME_DROP_FAIL_Z = -2.0

# The generator places trips so the newest event is normally an hour or two old, and the
# CI pipeline runs daily, so a real stall means data is most of a day stale.
FRESHNESS_WARN_SECONDS = 12 * 3600   # newest event older than this -> warn
FRESHNESS_FAIL_SECONDS = 24 * 3600   # older than this -> fail

# Rare-event rates (disengagements) are jumpy at small volume, so drift must be both
# statistically out of band AND a material relative move off the baseline before it flags.
# Tukey convention: 1.5*IQR is a mild outlier (warn), 3.0*IQR is an extreme outlier (fail).
DRIFT_WARN_IQR = 1.5
DRIFT_FAIL_IQR = 3.0
DRIFT_WARN_Z = 2.0
DRIFT_FAIL_Z = 3.0
DRIFT_WARN_REL = 0.5            # value must be >50% off the baseline mean to warn
DRIFT_FAIL_REL = 1.0           # and >100% off (at least doubled or halved) to fail

VEHICLE_DROPOUT_FAIL_FRAC = 0.10   # this share of last run's vehicles missing -> fail

GREEN, YELLOW, RED, RESET = "\033[32m", "\033[33m", "\033[31m", "\033[0m"
STATUS_COLOR = {"pass": GREEN, "warn": YELLOW, "fail": RED}


# ---------------------------------------------------------------------------
# Result helper
# ---------------------------------------------------------------------------

def result(check, tier, status, observed=None, baseline=None, threshold=None, detail=""):
    return {
        "check": check, "tier": tier, "status": status,
        "observed": observed, "baseline": baseline, "threshold": threshold, "detail": detail,
    }


# ---------------------------------------------------------------------------
# Tier 1: deterministic hard gates (pass or fail)
# ---------------------------------------------------------------------------

def schema_check(con):
    rows = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{SILVER_PARQUET}')").fetchall()
    actual = {r[0]: r[1] for r in rows}
    problems = []
    for col, expected in EXPECTED_SCHEMA.items():
        if col not in actual:
            problems.append(f"{col} missing")
        elif actual[col] != expected:
            problems.append(f"{col} is {actual[col]} not {expected}")
    status = "fail" if problems else "pass"
    detail = "; ".join(problems) if problems else "all 11 columns present and correctly typed"
    return result("schema", 1, status, observed=problems or "ok",
                  threshold="expected 11-field schema", detail=detail)


def null_check(con):
    counts = {}
    for field in REQUIRED_FIELDS:
        counts[field] = con.execute(
            f"SELECT count(*) FROM read_parquet('{SILVER_PARQUET}') WHERE {field} IS NULL"
        ).fetchone()[0]
    bad = {f: n for f, n in counts.items() if n > 0}
    status = "fail" if bad else "pass"
    detail = ", ".join(f"{f}={n} null" for f, n in bad.items()) if bad else "no nulls in required fields"
    return result("null", 1, status, observed=counts, threshold="0 nulls in required fields", detail=detail)


def range_check(con):
    bounds = {
        "speed_mph": f"speed_mph < {SPEED_MIN} OR speed_mph > {SPEED_MAX}",
        "accel": f"accel < {ACCEL_MIN} OR accel > {ACCEL_MAX}",
        "lat": f"lat < {LAT_MIN} OR lat > {LAT_MAX}",
        "lon": f"lon < {LON_MIN} OR lon > {LON_MAX}",
    }
    violations = {}
    for field, condition in bounds.items():
        n = con.execute(
            f"SELECT count(*) FROM read_parquet('{SILVER_PARQUET}') WHERE {condition}"
        ).fetchone()[0]
        if n > 0:
            violations[field] = n
    status = "fail" if violations else "pass"
    detail = ", ".join(f"{f}={n}" for f, n in violations.items()) if violations else "all values in range"
    return result("range", 1, status, observed=violations or "ok",
                  threshold="speed 0..120, accel -10..10, lat/lon in box", detail=detail)


def uniqueness_check(con):
    total, distinct = con.execute(
        f"SELECT count(*), count(DISTINCT event_id) FROM read_parquet('{SILVER_PARQUET}')"
    ).fetchone()
    dupes = total - distinct
    status = "fail" if dupes > 0 else "pass"
    detail = f"{dupes} duplicate event_id" if dupes else "event_id is unique"
    return result("uniqueness", 1, status, observed=dupes, threshold="0 duplicate event_id", detail=detail)


def referential_integrity_check(con):
    orphans = {}
    for key, dim in FACT_FOREIGN_KEYS:
        n = con.execute(f"""
            SELECT count(*)
            FROM read_parquet('{FACT}') f
            LEFT JOIN read_parquet('{GOLD_DIR}/{dim}.parquet') d USING ({key})
            WHERE d.{key} IS NULL
        """).fetchone()[0]
        if n > 0:
            orphans[dim] = n
    status = "fail" if orphans else "pass"
    detail = ", ".join(f"{d}={n} orphans" for d, n in orphans.items()) if orphans else "all fact FKs resolve"
    return result("referential_integrity", 1, status, observed=orphans or "ok",
                  threshold="0 orphan foreign keys", detail=detail)


# ---------------------------------------------------------------------------
# Run summary and the rolling baseline
# ---------------------------------------------------------------------------

def summarize_run(con):
    # Compute the freshness lag in SQL (seconds), so DuckDB never hands a timezone-aware
    # timestamp back to Python, which would otherwise need the pytz module installed.
    row_count, diseng_rate, ap_share, vehicles, max_ts_lag = con.execute(f"""
        SELECT
            count(*),
            avg(CASE WHEN disengagement THEN 1 ELSE 0 END),
            avg(CASE WHEN autopilot_engaged THEN 1 ELSE 0 END),
            count(DISTINCT vehicle_id),
            epoch(now()) - epoch(max(ts))
        FROM read_parquet('{SILVER_PARQUET}')
    """).fetchone()

    summary = {
        "run_ts": datetime.now(timezone.utc).isoformat(),
        "row_count": int(row_count),
        "disengagement_rate": float(diseng_rate),
        "autopilot_share": float(ap_share),
        "max_ts_lag_seconds": float(max_ts_lag),
        "distinct_vehicle_count": int(vehicles),
    }
    for field in REQUIRED_FIELDS:
        summary[f"null_rate_{field}"] = float(con.execute(
            f"SELECT avg(CASE WHEN {field} IS NULL THEN 1 ELSE 0 END) FROM read_parquet('{SILVER_PARQUET}')"
        ).fetchone()[0])
    return summary


def current_vehicles(con):
    rows = con.execute(f"SELECT DISTINCT vehicle_id FROM read_parquet('{SILVER_PARQUET}')").fetchall()
    return {r[0] for r in rows}


def load_history():
    return pd.read_parquet(RUN_HISTORY) if os.path.exists(RUN_HISTORY) else pd.DataFrame()


def append_history(summary):
    combined = pd.concat([load_history(), pd.DataFrame([summary])], ignore_index=True)
    os.makedirs(os.path.dirname(RUN_HISTORY), exist_ok=True)
    combined.to_parquet(RUN_HISTORY, index=False)


def load_run_vehicles():
    if not os.path.exists(RUN_VEHICLES):
        return None
    return set(pd.read_parquet(RUN_VEHICLES)["vehicle_id"])


def save_run_vehicles(vehicles):
    os.makedirs(os.path.dirname(RUN_VEHICLES), exist_ok=True)
    pd.DataFrame({"vehicle_id": sorted(vehicles)}).to_parquet(RUN_VEHICLES, index=False)


# ---------------------------------------------------------------------------
# Tier 2: statistical anomaly checks (pure functions over a value + prior history)
# ---------------------------------------------------------------------------

def zscore(value, mean, std):
    return (value - mean) / std if std and std > 0 else 0.0


def flag_volume_drop(current, history):
    if len(history) < MIN_HISTORY:
        return result("volume_drop", 2, "pass", observed=current, detail="insufficient history")
    mean, std = float(history.mean()), float(history.std())
    pct_below = (mean - current) / mean if mean > 0 else 0.0
    z = zscore(current, mean, std)
    if pct_below > VOLUME_DROP_FAIL_FRAC or z < VOLUME_DROP_FAIL_Z:
        status = "fail"
    elif pct_below > VOLUME_DROP_WARN_FRAC or z < VOLUME_DROP_WARN_Z:
        status = "warn"
    else:
        status = "pass"
    return result("volume_drop", 2, status, observed=current,
                  baseline={"mean": round(mean, 1), "std": round(std, 1)},
                  threshold=f">{int(VOLUME_DROP_FAIL_FRAC*100)}% below mean or z<{VOLUME_DROP_FAIL_Z}",
                  detail=f"row_count {current} is {pct_below*100:.0f}% below mean {mean:.0f} (z={z:.2f})")


def flag_freshness(lag_seconds):
    if lag_seconds > FRESHNESS_FAIL_SECONDS:
        status = "fail"
    elif lag_seconds > FRESHNESS_WARN_SECONDS:
        status = "warn"
    else:
        status = "pass"
    return result("freshness", 2, status, observed=round(lag_seconds, 1),
                  threshold=f"warn>{FRESHNESS_WARN_SECONDS}s fail>{FRESHNESS_FAIL_SECONDS}s",
                  detail=f"newest event {lag_seconds/3600:.2f}h old")


def flag_drift(name, current, history):
    if len(history) < MIN_HISTORY:
        return result(f"drift_{name}", 2, "pass", observed=round(current, 6), detail="insufficient history")
    q1, q3 = float(history.quantile(0.25)), float(history.quantile(0.75))
    iqr = q3 - q1
    mean, std = float(history.mean()), float(history.std())
    z = zscore(current, mean, std)
    rel = abs(current - mean) / mean if mean > 0 else (float("inf") if current > 0 else 0.0)

    out_fail = current < q1 - DRIFT_FAIL_IQR * iqr or current > q3 + DRIFT_FAIL_IQR * iqr or (std > 0 and abs(z) > DRIFT_FAIL_Z)
    out_warn = current < q1 - DRIFT_WARN_IQR * iqr or current > q3 + DRIFT_WARN_IQR * iqr or (std > 0 and abs(z) > DRIFT_WARN_Z)
    if out_fail and rel > DRIFT_FAIL_REL:
        status = "fail"
    elif out_warn and rel > DRIFT_WARN_REL:
        status = "warn"
    else:
        status = "pass"
    return result(f"drift_{name}", 2, status, observed=round(current, 6),
                  baseline={"mean": round(mean, 6), "q1": round(q1, 6), "q3": round(q3, 6)},
                  threshold=f"outside {DRIFT_FAIL_IQR}*IQR or |z|>{DRIFT_FAIL_Z}, and >{int(DRIFT_FAIL_REL*100)}% off baseline",
                  detail=f"{name}={current:.6f} vs mean {mean:.6f} (z={z:.2f}, {rel*100:.0f}% off)")


def flag_vehicle_dropout(current_set, last_set):
    if not last_set:
        return result("vehicle_dropout", 2, "pass", observed=len(current_set),
                      detail="no prior run to compare")
    dropped = last_set - current_set
    frac = len(dropped) / len(last_set)
    if frac >= VEHICLE_DROPOUT_FAIL_FRAC:
        status = "fail"
    elif dropped:
        status = "warn"
    else:
        status = "pass"
    return result("vehicle_dropout", 2, status, observed=len(dropped),
                  baseline={"last_run_vehicles": len(last_set)},
                  threshold=f"fail if >={int(VEHICLE_DROPOUT_FAIL_FRAC*100)}% of last run absent",
                  detail=f"{len(dropped)} of {len(last_set)} vehicles dropped ({frac*100:.0f}%)")


def run_tier2(summary, current_set, history, last_set):
    """Compare this run's summary to the trailing window of prior runs."""
    window = history.tail(TRAILING_WINDOW)

    def col(name):
        return window[name] if name in window else pd.Series([], dtype=float)

    checks = [
        flag_volume_drop(summary["row_count"], col("row_count")),
        flag_freshness(summary["max_ts_lag_seconds"]),
        flag_drift("disengagement_rate", summary["disengagement_rate"], col("disengagement_rate")),
    ]
    for field in REQUIRED_FIELDS:
        key = f"null_rate_{field}"
        checks.append(flag_drift(key, summary[key], col(key)))
    checks.append(flag_vehicle_dropout(current_set, last_set))
    return checks


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def overall_status(results):
    if any(r["status"] == "fail" for r in results):
        return "fail"
    if any(r["status"] == "warn" for r in results):
        return "warn"
    return "pass"


def write_report(summary, results):
    report = {
        "run_ts": summary["run_ts"],
        "overall_status": overall_status(results),
        "summary": summary,
        "checks": results,
    }
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as fh:
        json.dump(report, fh, indent=2, default=str)


def print_summary(summary, results):
    use_color = sys.stdout.isatty()

    def paint(text, status):
        return f"{STATUS_COLOR[status]}{text}{RESET}" if use_color else text

    print("FleetSignal quality report")
    print(f"  rows={summary['row_count']:,}  vehicles={summary['distinct_vehicle_count']}  "
          f"disengagement_rate={summary['disengagement_rate']:.4f}  "
          f"autopilot_share={summary['autopilot_share']:.3f}")
    for r in results:
        print(paint(f"  [{r['status'].upper():^4}] tier{r['tier']} {r['check']}: {r['detail']}", r["status"]))
    overall = overall_status(results)
    print(paint(f"  OVERALL: {overall.upper()}", overall))


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(description="Run the two-tier data quality checks.")
    parser.add_argument("--no-append", action="store_true",
                        help="check against the baseline without recording this run")
    return parser.parse_args()


def main():
    args = parse_args()
    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'")

    results = [
        schema_check(con),
        null_check(con),
        range_check(con),
        uniqueness_check(con),
        referential_integrity_check(con),
    ]

    summary = summarize_run(con)
    vehicles = current_vehicles(con)

    # Read prior runs BEFORE recording this one, so this run is compared to its past.
    history = load_history()
    last_vehicles = load_run_vehicles()
    results += run_tier2(summary, vehicles, history, last_vehicles)

    if not args.no_append:
        append_history(summary)
        save_run_vehicles(vehicles)

    write_report(summary, results)
    print_summary(summary, results)

    sys.exit(1 if overall_status(results) == "fail" else 0)


if __name__ == "__main__":
    main()
