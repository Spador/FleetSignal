"""FleetSignal dashboard: three metric cards, a scenario hotspot map, a safety trend.

Reads the gold metric table, the scenario files, and the gold fact, all with pandas.
The data-loading functions are pure (no Streamlit calls), so they can be imported and
checked on their own. Streamlit rendering happens in main().

Run:
    streamlit run app.py
"""

import os

import altair as alt
import pandas as pd
import pydeck as pdk
import streamlit as st


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

GOLD_DIR = "data/gold"
METRIC_SAFETY = f"{GOLD_DIR}/metric_safety.parquet"
FACT = f"{GOLD_DIR}/fact_drive_events.parquet"
SCENARIO_FILES = [
    "scenarios/disengagement_clusters.parquet",
    "scenarios/hard_brakes_bad_weather.parquet",
    "scenarios/highspeed_autopilot_poor_weather.parquet",
]

# A no-token dark basemap, so hotspots glow without needing a Mapbox key.
MAP_STYLE = "https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json"
MAP_HEX_RADIUS_M = 150
MAP_ELEVATION_SCALE = 8
MAP_ZOOM = 10.5
MAP_PITCH = 45

ACCENT = "#19E0C8"
MANUAL_LABEL = "manual baseline (0 by definition)"


# ---------------------------------------------------------------------------
# Data loading (pure pandas, no Streamlit)
# ---------------------------------------------------------------------------

def _slice(df, dimension, value):
    rows = df[(df.slice_dimension == dimension) & (df.slice_value == value)]
    return rows.iloc[0] if len(rows) else None


def load_metric_values():
    df = pd.read_parquet(METRIC_SAFETY)
    overall = _slice(df, "overall", "all")
    autopilot = _slice(df, "mode", "autopilot")
    manual = _slice(df, "mode", "manual")
    return {
        "miles_per_disengagement": overall["miles_per_disengagement"],
        "intervention_rate_per_1k": overall["intervention_rate_per_1k"],
        "total_miles": overall["total_miles"],
        "autopilot_miles": autopilot["total_miles"] if autopilot is not None else 0.0,
        "manual_miles": manual["total_miles"] if manual is not None else 0.0,
    }


def load_scenario_points():
    """All flagged scenario locations, as plain lat/lon points for the hex map."""
    frames = [
        pd.read_parquet(path, columns=["lat_bucket", "lon_bucket"])
        for path in SCENARIO_FILES if os.path.exists(path)
    ]
    if not frames:
        return pd.DataFrame(columns=["lat", "lon"])
    points = pd.concat(frames, ignore_index=True)
    return points.rename(columns={"lat_bucket": "lat", "lon_bucket": "lon"})


def load_hourly_trend():
    """Interventions per 1k autopilot miles, by event hour, plus the manual zero line."""
    fact = pd.read_parquet(FACT, columns=["ts", "autopilot_engaged", "disengagement", "miles_segment"])
    autopilot = fact[fact.autopilot_engaged].copy()
    autopilot["hour"] = autopilot["ts"].dt.tz_localize(None).dt.floor("h")

    per_hour = autopilot.groupby("hour").agg(
        autopilot_miles=("miles_segment", "sum"),
        disengagements=("disengagement", "sum"),
    ).reset_index()
    per_hour["rate"] = per_hour["disengagements"] / per_hour["autopilot_miles"] * 1000
    per_hour.loc[per_hour["autopilot_miles"] <= 0, "rate"] = 0

    autopilot_line = per_hour[["hour", "rate"]].assign(series="autopilot")
    manual_line = per_hour[["hour"]].assign(rate=0.0, series=MANUAL_LABEL)
    return pd.concat([autopilot_line, manual_line], ignore_index=True)


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------

def render_cards(metrics):
    left, middle, right = st.columns(3)
    left.metric("Miles per disengagement", f"{metrics['miles_per_disengagement']:,.0f}",
                help="Autopilot only. A disengagement cannot occur in manual mode.")
    middle.metric("Interventions per 1k mi", f"{metrics['intervention_rate_per_1k']:.2f}",
                  help="Autopilot only. Disengagements per 1,000 autopilot miles.")
    right.metric("Total miles processed", f"{metrics['total_miles']:,.0f}",
                 delta=f"{metrics['autopilot_miles'] - metrics['manual_miles']:+,.0f} autopilot vs manual")


def render_map(points):
    layer = pdk.Layer(
        "HexagonLayer",
        data=points,
        get_position=["lon", "lat"],
        radius=MAP_HEX_RADIUS_M,
        elevation_scale=MAP_ELEVATION_SCALE,
        elevation_range=[0, 1000],
        extruded=True,
        coverage=0.9,
        pickable=True,
        auto_highlight=True,
    )
    view = pdk.ViewState(
        latitude=float(points["lat"].mean()),
        longitude=float(points["lon"].mean()),
        zoom=MAP_ZOOM,
        pitch=MAP_PITCH,
    )
    st.pydeck_chart(pdk.Deck(
        layers=[layer], initial_view_state=view, map_style=MAP_STYLE,
        tooltip={"text": "flagged events here: {elevationValue}"},
    ))


def render_trend(trend):
    chart = alt.Chart(trend).mark_line().encode(
        x=alt.X("hour:T", title="event hour (UTC)"),
        y=alt.Y("rate:Q", title="interventions per 1k autopilot miles"),
        color=alt.Color("series:N", title=None,
                        scale=alt.Scale(domain=["autopilot", MANUAL_LABEL], range=[ACCENT, "#888888"])),
    ).properties(height=320)
    st.altair_chart(chart, use_container_width=True)


def main():
    st.set_page_config(page_title="FleetSignal", layout="wide")
    st.title("FleetSignal")
    st.caption("Synthetic fleet telemetry: the safety metric, flagged scenarios, and the autopilot safety trend.")

    if not (os.path.exists(METRIC_SAFETY) and os.path.exists(FACT)):
        st.warning("No gold data found. Run the pipeline first:  python3 orchestrate.py")
        return

    render_cards(load_metric_values())

    st.subheader("Flagged scenario hotspots")
    points = load_scenario_points()
    if len(points):
        render_map(points)
    else:
        st.info("No scenario files yet. Run scenarios.sql, or the orchestrator.")

    st.subheader("Autopilot safety trend (interventions per 1k autopilot miles, by hour)")
    render_trend(load_hourly_trend())


if __name__ == "__main__":
    main()
