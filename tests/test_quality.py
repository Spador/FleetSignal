"""Unit tests for the Tier 2 statistical functions in quality.py.

These are the most logic-heavy part, so they get pinned down here. Each function is pure
(a value plus the prior history in, a status out), so the tests need no data files.
"""

import os
import sys

import pandas as pd

# Put the repo root on the path so `import quality` works under any pytest invocation.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from quality import (
    flag_volume_drop, flag_freshness, flag_drift, flag_vehicle_dropout, zscore,
    FRESHNESS_WARN_SECONDS, FRESHNESS_FAIL_SECONDS,
)


def test_zscore_handles_zero_std():
    assert zscore(5, 5, 0) == 0.0
    assert zscore(10, 0, 5) == 2.0


def test_volume_drop_insufficient_history_passes():
    assert flag_volume_drop(6000, pd.Series([20000, 20000]))["status"] == "pass"


def test_volume_drop_fails_on_big_drop():
    assert flag_volume_drop(6000, pd.Series([20000] * 5))["status"] == "fail"   # 70% below mean


def test_volume_drop_passes_when_stable():
    assert flag_volume_drop(20000, pd.Series([20000] * 5))["status"] == "pass"


def test_freshness_bands():
    assert flag_freshness(60)["status"] == "pass"
    assert flag_freshness(FRESHNESS_WARN_SECONDS + 1)["status"] == "warn"
    assert flag_freshness(FRESHNESS_FAIL_SECONDS + 1)["status"] == "fail"


def test_drift_passes_on_normal_wobble():
    hist = pd.Series([0.0005, 0.0004, 0.0006, 0.0005, 0.0007])
    assert flag_drift("disengagement_rate", 0.0006, hist)["status"] == "pass"


def test_drift_fails_on_spike():
    hist = pd.Series([0.0005, 0.0004, 0.0006, 0.0005, 0.0007])
    assert flag_drift("disengagement_rate", 0.03, hist)["status"] == "fail"


def test_drift_fails_on_null_rate_jump():
    # baseline null rate is exactly zero, then 5% of rows go null
    assert flag_drift("null_rate_speed_mph", 0.05, pd.Series([0.0, 0.0, 0.0, 0.0]))["status"] == "fail"


def test_vehicle_dropout_fails_on_big_drop():
    last = {f"VH_{i:05d}" for i in range(1, 51)}      # 50 vehicles
    current = {f"VH_{i:05d}" for i in range(1, 41)}   # last 10 dropped (20%)
    assert flag_vehicle_dropout(current, last)["status"] == "fail"


def test_vehicle_dropout_passes_when_complete():
    fleet = {f"VH_{i:05d}" for i in range(1, 51)}
    assert flag_vehicle_dropout(fleet, fleet)["status"] == "pass"


def test_vehicle_dropout_no_prior_passes():
    assert flag_vehicle_dropout({"VH_00001"}, None)["status"] == "pass"
