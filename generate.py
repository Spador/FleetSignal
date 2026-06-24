"""Generate synthetic fleet telemetry and write it to the bronze layer.

The generation logic is pure standard-library Python so every line is easy to
explain. pandas and pyarrow are used only at the very end to write parquet.

Run:
    python3 generate.py --vehicles 200 --events 200000 --seed 7
    python3 generate.py --inject-anomaly spike_disengagements
"""

import argparse
import math
import random
import shutil
import time
from datetime import datetime, timedelta, timezone

import pandas as pd


# ---------------------------------------------------------------------------
# Config. Every tunable lives here, named, so nothing is a magic number below.
# ---------------------------------------------------------------------------

# A real-ish bounding box (San Francisco) so the map later looks like a city.
LAT_MIN, LAT_MAX = 37.70, 37.83
LON_MIN, LON_MAX = -122.52, -122.36

AUTOPILOT_SHARE = 0.60          # roughly 60 percent of events are on autopilot
SPEED_MIN, SPEED_MAX = 0.0, 120.0
ACCEL_MIN, ACCEL_MAX = -10.0, 10.0

# Weather picked once per vehicle so a trip has coherent conditions.
WEATHER_WEIGHTS = {"clear": 0.60, "rain": 0.20, "snow": 0.10, "fog": 0.10}

# Mean acceleration per weather. Bad weather skews negative, which produces more
# hard brakes. That is the "hard brakes correlate with rain and snow" rule.
WEATHER_ACCEL_MEAN = {"clear": 0.0, "rain": -1.5, "snow": -2.0, "fog": -0.8}
ACCEL_STD = 2.0
HARD_BRAKE_ACCEL_THRESHOLD = -4.0   # accel below this counts as a hard brake

# Disengagements are rare but get likelier in bad weather and right after a hard
# brake. That is the "disengagements correlate with bad weather and hard brakes" rule.
DISENGAGEMENT_BASE_RATE = 1 / 3000   # per autopilot event in clear, no hard brake
DISENGAGE_WEATHER_MULT = 4.0         # applied when weather is not clear
DISENGAGE_HARD_BRAKE_MULT = 5.0      # applied when the event is a hard brake

# How a track moves between events.
CADENCE_MIN, CADENCE_MAX = 3.0, 7.0  # seconds between events
AVG_CADENCE = (CADENCE_MIN + CADENCE_MAX) / 2.0
SPEED_STEP_STD = 4.0                 # mph wobble per step
HEADING_DRIFT_STD = 0.30             # radians of steering drift per step
MILES_PER_DEGREE = 69.0              # rough miles per degree of lat/lon

DEFAULT_VEHICLES = 200
DEFAULT_EVENTS = 200_000
DEFAULT_DAYS = 3                     # spread of trip start times, ending now
DEFAULT_OUT = "data/bronze"

# Knobs for each injected fault.
DROP_VEHICLE_FRACTION = 0.20         # drop_vehicles: omit this share of the fleet
DISENGAGE_SPIKE_FACTOR = 50.0        # spike_disengagements: multiply the rate
NULL_FIELD = "speed_mph"             # null_field: which required field to blank
NULL_FRACTION = 0.05                 # null_field: share of rows to blank
VOLUME_DROP_FRACTION = 0.30          # volume_drop: keep only this share of events

# The 11 telemetry fields from spec section 4, in order.
SCHEMA_COLUMNS = [
    "event_id", "vehicle_id", "ts", "speed_mph", "accel", "lat", "lon",
    "autopilot_engaged", "disengagement", "hard_brake", "weather",
]

ANOMALY_MODES = ["drop_vehicles", "spike_disengagements", "null_field", "volume_drop"]


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------

def clamp(value, low, high):
    return max(low, min(high, value))


def pick_weather(rng):
    conditions = list(WEATHER_WEIGHTS.keys())
    weights = list(WEATHER_WEIGHTS.values())
    return rng.choices(conditions, weights=weights, k=1)[0]


def split_events(total, n_vehicles):
    """Spread total events across vehicles as evenly as possible."""
    base = total // n_vehicles
    remainder = total % n_vehicles
    return [base + (1 if i < remainder else 0) for i in range(n_vehicles)]


# ---------------------------------------------------------------------------
# Track simulation
# ---------------------------------------------------------------------------

def trip_start_offset(rng, n_events, window_seconds):
    """How many seconds ago a trip starts, picked so it finishes by now and
    still falls within the last `days`."""
    duration = n_events * AVG_CADENCE
    if duration >= window_seconds:
        return window_seconds          # very long trips start at the oldest edge
    return rng.uniform(duration, window_seconds)


def new_vehicle(rng, start_ts):
    """Starting state for one vehicle: a random spot, speed and heading."""
    return {
        "ts": start_ts,
        "lat": rng.uniform(LAT_MIN, LAT_MAX),
        "lon": rng.uniform(LON_MIN, LON_MAX),
        "speed": rng.uniform(0, 60),
        "heading": rng.uniform(0, 2 * math.pi),
        "weather": pick_weather(rng),
    }


def step(state, rng, spike):
    """Advance the track one event and return that event as a dict.

    `spike` multiplies the disengagement rate (1.0 normally).
    """
    dt = rng.uniform(CADENCE_MIN, CADENCE_MAX)
    state["ts"] = state["ts"] + timedelta(seconds=dt)
    state["speed"] = clamp(state["speed"] + rng.gauss(0, SPEED_STEP_STD), SPEED_MIN, SPEED_MAX)

    accel = clamp(rng.gauss(WEATHER_ACCEL_MEAN[state["weather"]], ACCEL_STD), ACCEL_MIN, ACCEL_MAX)

    # Move forward along the current heading, distance = speed over the time gap.
    state["heading"] += rng.gauss(0, HEADING_DRIFT_STD)
    degrees = (state["speed"] * (dt / 3600.0)) / MILES_PER_DEGREE
    state["lat"] = clamp(state["lat"] + degrees * math.cos(state["heading"]), LAT_MIN, LAT_MAX)
    state["lon"] = clamp(state["lon"] + degrees * math.sin(state["heading"]), LON_MIN, LON_MAX)

    hard_brake = accel < HARD_BRAKE_ACCEL_THRESHOLD
    autopilot = rng.random() < AUTOPILOT_SHARE

    disengagement = False
    if autopilot:
        rate = DISENGAGEMENT_BASE_RATE * spike
        if state["weather"] != "clear":
            rate *= DISENGAGE_WEATHER_MULT
        if hard_brake:
            rate *= DISENGAGE_HARD_BRAKE_MULT
        disengagement = rng.random() < rate

    return {
        "vehicle_id": None,  # filled in by the caller
        "ts": state["ts"],
        "speed_mph": round(state["speed"], 2),
        "accel": round(accel, 2),
        "lat": round(state["lat"], 6),
        "lon": round(state["lon"], 6),
        "autopilot_engaged": autopilot,
        "disengagement": disengagement,
        "hard_brake": hard_brake,
        "weather": state["weather"],
    }


def generate(vehicles, events, days, seed, anomaly):
    """Build the full list of event rows for the fleet."""
    rng = random.Random(seed)

    vehicle_ids = [f"VH_{i:05d}" for i in range(1, vehicles + 1)]
    if anomaly == "drop_vehicles":
        keep = max(1, len(vehicle_ids) - int(len(vehicle_ids) * DROP_VEHICLE_FRACTION))
        vehicle_ids = vehicle_ids[:keep]
    if anomaly == "volume_drop":
        events = int(events * VOLUME_DROP_FRACTION)

    spike = DISENGAGE_SPIKE_FACTOR if anomaly == "spike_disengagements" else 1.0

    now = datetime.now(timezone.utc)
    window_seconds = days * 86400

    rows = []
    for vehicle_id, n_events in zip(vehicle_ids, split_events(events, len(vehicle_ids))):
        start_ts = now - timedelta(seconds=trip_start_offset(rng, n_events, window_seconds))
        state = new_vehicle(rng, start_ts)
        for _ in range(n_events):
            event = step(state, rng, spike)
            event["vehicle_id"] = vehicle_id
            rows.append(event)

    for event_id, row in enumerate(rows, start=1):
        row["event_id"] = event_id
    return rows


# ---------------------------------------------------------------------------
# Write the bronze layer
# ---------------------------------------------------------------------------

def write_bronze(rows, out_dir, anomaly, seed):
    df = pd.DataFrame(rows, columns=SCHEMA_COLUMNS)

    if anomaly == "null_field":
        blanked = df.sample(frac=NULL_FRACTION, random_state=seed).index
        df.loc[blanked, NULL_FIELD] = float("nan")

    # Partition key. Bronze is "raw events as generated, partitioned by date".
    df["date"] = df["ts"].dt.strftime("%Y-%m-%d")

    shutil.rmtree(out_dir, ignore_errors=True)   # regenerate from scratch every run
    df.to_parquet(out_dir, partition_cols=["date"], index=False)
    return df


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(description="Generate synthetic fleet telemetry into bronze.")
    parser.add_argument("--vehicles", type=int, default=DEFAULT_VEHICLES)
    parser.add_argument("--events", type=int, default=DEFAULT_EVENTS)
    parser.add_argument("--days", type=int, default=DEFAULT_DAYS)
    parser.add_argument("--seed", type=int, default=None,
                        help="fix for a reproducible run; omit for natural run-to-run variance")
    parser.add_argument("--inject-anomaly", choices=ANOMALY_MODES, default=None,
                        dest="inject_anomaly")
    parser.add_argument("--out", default=DEFAULT_OUT)
    return parser.parse_args()


def main():
    args = parse_args()
    start = time.time()

    rows = generate(args.vehicles, args.events, args.days, args.seed, args.inject_anomaly)
    df = write_bronze(rows, args.out, args.inject_anomaly, args.seed)

    elapsed = time.time() - start
    print(f"wrote {len(df):,} events for {df.vehicle_id.nunique():,} vehicles "
          f"across {df.date.nunique()} date partitions -> {args.out}")
    print(f"  autopilot share : {df.autopilot_engaged.mean():.3f}")
    print(f"  disengagements  : {int(df.disengagement.sum()):,}")
    print(f"  hard brakes     : {int(df.hard_brake.sum()):,}")
    if args.inject_anomaly:
        print(f"  injected fault  : {args.inject_anomaly}")
    print(f"  elapsed         : {elapsed:.1f}s")


if __name__ == "__main__":
    main()
