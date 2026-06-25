"""Run the whole FleetSignal pipeline end to end, with the quality gates as checkpoints.

Stages run in order, each timed and retried once on error. The Tier 1 gate sits between
silver and gold: bad data halts there, before any modeling compute is spent (prevention,
not just detection). Referential integrity is checked right after gold. Tier 2 anomaly
detection runs at the end against the rolling baseline. Any gate failure halts the run and
exits nonzero, which is what makes CI mark it failed.

Run:
    python3 orchestrate.py --vehicles 50 --events 20000
    python3 orchestrate.py --inject-anomaly spike_disengagements
"""

import argparse
import os
import sys
import time

import generate
import pipeline
import quality


SCENARIOS_DIR = "scenarios"
METRICS_SQL = "metrics.sql"
SCENARIOS_SQL = "scenarios.sql"
RETRIES = 1   # one retry per work stage before giving up

RESULTS = []   # every quality check result, collected for the final report


def log(msg):
    print(f"[orchestrate] {msg}", flush=True)


def finalize():
    """Write the quality report from whatever results we have so far."""
    summary = {}
    try:
        con = quality.connect()
        summary = quality.summarize_run(con)
        con.close()
    except Exception:
        pass
    quality.write_report(summary, RESULTS)


def timed_retry(name, fn):
    """Run a work stage with a timer and one retry on error; halt if it keeps failing."""
    for attempt in range(1, RETRIES + 2):
        start = time.time()
        try:
            out = fn()
            log(f"{name:<10} ok ({time.time() - start:.2f}s)")
            return out
        except Exception as error:
            log(f"{name:<10} error on attempt {attempt}: {error}")
    log(f"HALT: stage {name} failed after {RETRIES + 1} attempts")
    finalize()
    sys.exit(1)


def gate(name, checks):
    """A quality checkpoint: record the results, and halt the run if any check failed."""
    RESULTS.extend(checks)
    for check in checks:
        log(f"gate {name}: {check['check']} -> {check['status']}")
    failed = [c for c in checks if c["status"] == "fail"]
    if failed:
        log(f"HALT at {name}: " + ", ".join(c["check"] for c in failed))
        finalize()
        sys.exit(1)


def run_sql_file(path):
    con = quality.connect()
    con.execute(open(path).read())
    con.close()


def stage_generate(args):
    rows = generate.generate(args.vehicles, args.events, args.days, args.seed, args.inject_anomaly)
    generate.write_bronze(rows, generate.DEFAULT_OUT, args.inject_anomaly, args.seed)


def parse_args():
    parser = argparse.ArgumentParser(description="Run the whole FleetSignal pipeline end to end.")
    parser.add_argument("--vehicles", type=int, default=generate.DEFAULT_VEHICLES)
    parser.add_argument("--events", type=int, default=generate.DEFAULT_EVENTS)
    parser.add_argument("--days", type=int, default=generate.DEFAULT_DAYS)
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument("--inject-anomaly", choices=generate.ANOMALY_MODES, default=None,
                        dest="inject_anomaly")
    parser.add_argument("--no-append", action="store_true",
                        help="do not record this run in the baseline (for fault demos)")
    return parser.parse_args()


def main():
    args = parse_args()
    os.makedirs(SCENARIOS_DIR, exist_ok=True)
    log(f"start: vehicles={args.vehicles} events={args.events} anomaly={args.inject_anomaly}")

    # 1. generate -> bronze
    timed_retry("generate", lambda: stage_generate(args))

    # 2. silver, then the Tier 1 gate before we model anything
    timed_retry("silver", pipeline.build_silver)
    con = quality.connect()
    gate("tier1_silver", quality.tier1_silver_checks(con))
    con.close()

    # 3. gold, then the referential-integrity gate (needs the dims and fact)
    timed_retry("gold", pipeline.build_gold)
    con = quality.connect()
    gate("referential", [quality.referential_integrity_check(con)])
    con.close()

    # 4. analytics on the modeled layer
    timed_retry("metrics", lambda: run_sql_file(METRICS_SQL))
    timed_retry("scenarios", lambda: run_sql_file(SCENARIOS_SQL))

    # 5. Tier 2 anomaly detection against the rolling baseline
    con = quality.connect()
    summary = quality.summarize_run(con)
    vehicles = quality.current_vehicles(con)
    con.close()
    tier2 = quality.run_tier2(summary, vehicles, quality.load_history(), quality.load_run_vehicles())
    RESULTS.extend(tier2)
    for check in tier2:
        log(f"anomaly: {check['check']} -> {check['status']}")
    if not args.no_append:
        quality.append_history(summary)
        quality.save_run_vehicles(vehicles)

    quality.write_report(summary, RESULTS)
    status = quality.overall_status(RESULTS)
    log(f"OVERALL: {status.upper()}  (report: {quality.REPORT})")
    sys.exit(1 if status == "fail" else 0)


if __name__ == "__main__":
    main()
