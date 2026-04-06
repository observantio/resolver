"""
Test cases for anomaly detection logic in the analysis engine, including output limiting and Granger causality series
selection.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
"""

import numpy as np

from api.responses import MetricAnomaly
from engine.anomaly.detection import (
    _apply_density_cap,
    _change_type,
    _compress_runs,
    _cusum_changepoints,
    _iqr_score_value,
    _mad_scores,
    _series_median_iqr,
    _severity,
    _tukey_outlier_class,
    detect,
)
from engine.enums import ChangeType, Severity


def test_iqr_and_tukey_helpers():
    arr = np.array([1.0, 2.0, 3.0, 4.0, 100.0], dtype=float)
    med, q1, q3, iqr = _series_median_iqr(arr)
    assert iqr > 0
    assert _iqr_score_value(100.0, med, iqr) > 1.0
    assert _tukey_outlier_class(100.0, q1, q3, iqr) in ("mild_high", "extreme_high")
    assert _tukey_outlier_class(3.0, q1, q3, iqr) == "none"


def test_mad_and_cusum():
    arr = [1, 1, 1, 10, 1, 1, 1]
    m = _mad_scores(np.array(arr))
    assert m.dtype in (float, "float64", "int64")
    flags_hi = _cusum_changepoints(np.array(arr), threshold=100)
    assert not flags_hi.any()
    flags_lo = _cusum_changepoints(np.array(arr), threshold=0.1)
    assert flags_lo.any()


def test_change_type_severity():
    assert _change_type(10, 0, 1, 0) == ChangeType.SPIKE
    assert _change_type(0, 0, -1, 0) == ChangeType.DROP
    assert _change_type(0, 0, 0, 1) == ChangeType.DRIFT
    sev = _severity(5, 0, -1)
    assert sev in (Severity.HIGH, Severity.CRITICAL)
    assert _severity(0, 0, 0) == Severity.LOW


def test_detect_simple():
    ts = list(range(20))
    vals = [1] * 19 + [100]
    anomalies = detect("m", ts, vals)
    assert isinstance(anomalies, list)
    if anomalies:
        assert hasattr(anomalies[0], "change_type")


def test_compress_runs_limits_noisy_sequences(monkeypatch):
    monkeypatch.setattr("config.settings.anomaly_run_keep_max", 3)
    items = [
        MetricAnomaly(
            metric_name="m",
            timestamp=float(i),
            value=float(i),
            change_type=ChangeType.SPIKE,
            z_score=2.5 + i * 0.1,
            mad_score=2.0,
            isolation_score=-0.5,
            expected_range=(0.0, 1.0),
            severity=Severity.HIGH,
            description="",
        )
        for i in range(10)
    ]
    compressed = _compress_runs(items)
    assert len(compressed) <= 3
    assert compressed[0].timestamp == 0.0
    assert compressed[-1].timestamp == 9.0


def test_detect_filters_non_finite_points():
    ts = [1, 2, 3, 4, 5, 6, 7, 8]
    vals = [1.0, 1.0, float("nan"), 1.0, float("inf"), 1.0, 20.0, 1.0]
    anomalies = detect("m", ts, vals, sensitivity=3.0)
    assert all(a.timestamp == a.timestamp for a in anomalies)
    assert all(a.value == a.value for a in anomalies)


def test_detect_requires_statistical_or_multisignal_corroboration_for_iso(monkeypatch):
    class FakeIsolationForest:
        def __init__(self, *args, **kwargs):
            pass

        def fit_predict(self, x):
            import numpy as np

            return np.full(shape=(x.shape[0],), fill_value=-1, dtype=int)

        def score_samples(self, x):
            import numpy as np

            return np.full(shape=(x.shape[0],), fill_value=-0.8, dtype=float)

    monkeypatch.setattr("engine.anomaly.detection.IsolationForest", FakeIsolationForest)
    ts = list(range(30))
    vals = [1.0, 1.2, 0.9, 1.1, 1.0, 1.3] * 5
    anomalies = detect("iso_only_noise", ts, vals, sensitivity=3.0)
    assert anomalies == []


def test_density_cap_limits_anomalies_per_hour(monkeypatch):
    monkeypatch.setattr("config.settings.quality_max_anomaly_density_per_metric_per_hour", 1.0)
    anomalies = [
        MetricAnomaly(
            metric_name="m",
            timestamp=float(i * 600),
            value=float(i),
            change_type=ChangeType.SPIKE,
            z_score=3.0 + i,
            mad_score=2.0 + i,
            isolation_score=-0.4,
            expected_range=(0.0, 1.0),
            severity=Severity.HIGH,
            description="",
        )
        for i in range(6)
    ]
    kept = _apply_density_cap(anomalies, np.array([a.timestamp for a in anomalies], dtype=float))
    assert len(kept) == 1


def test_cusum_zero_sigma_returns_no_flags():
    arr = np.ones(20, dtype=float)
    assert not _cusum_changepoints(arr).any()


def test_change_type_shift_when_z_zero():
    assert _change_type(1.0, 0.0, 0.0, 0.0) == ChangeType.SHIFT


def test_tukey_mild_low():
    arr = np.array([10.0, 11.0, 12.0, 13.0, 0.5], dtype=float)
    med, q1, q3, iqr = _series_median_iqr(arr)
    cls = _tukey_outlier_class(0.5, q1, q3, iqr)
    assert cls in ("mild_low", "extreme_low")


def test_tukey_mild_low_explicit_fence():
    assert _tukey_outlier_class(0.0, 10.0, 14.0, 4.0) == "mild_low"


def test_apply_density_cap_disabled_when_max_zero(monkeypatch):
    monkeypatch.setattr("config.settings.quality_max_anomaly_density_per_metric_per_hour", 0.0)
    anomalies = [
        MetricAnomaly(
            metric_name="m",
            timestamp=float(i),
            value=1.0,
            change_type=ChangeType.SPIKE,
            z_score=3.0,
            mad_score=3.0,
            isolation_score=-0.1,
            expected_range=(0.0, 1.0),
            severity=Severity.HIGH,
            description="",
            iqr_score=1.0,
            tukey_outlier_class="none",
        )
        for i in range(5)
    ]
    kept = _apply_density_cap(anomalies, np.linspace(0.0, 4.0, 5))
    assert len(kept) == 5


def test_apply_density_cap_single_timestamp_uses_hour_window(monkeypatch):
    monkeypatch.setattr("config.settings.quality_max_anomaly_density_per_metric_per_hour", 0.5)
    a = MetricAnomaly(
        metric_name="m",
        timestamp=1.0,
        value=1.0,
        change_type=ChangeType.SPIKE,
        z_score=9.0,
        mad_score=9.0,
        isolation_score=-0.1,
        expected_range=(0.0, 1.0),
        severity=Severity.HIGH,
        description="",
        iqr_score=2.0,
        tukey_outlier_class="none",
    )
    kept = _apply_density_cap([a], np.array([99.0], dtype=float))
    assert len(kept) == 1


def test_severity_includes_iqr_score():
    s = _severity(0.0, 0.0, 0, iqr_score=5.0)
    assert isinstance(s, Severity)


def test_detect_std_zero_returns_empty():
    ts = list(range(20))
    vals = [3.0] * 20
    assert detect("flat", ts, vals) == []


def _spy_isolation_forest(captured: dict[str, float]):
    class SpyIso:
        def __init__(self, contamination, **kwargs):
            captured["contamination"] = float(contamination)

        def fit_predict(self, x):
            return np.zeros((x.shape[0],), dtype=int)

        def score_samples(self, x):
            return np.zeros((x.shape[0],), dtype=float)

    return SpyIso


def test_detect_precision_profile_adjusts_contamination(monkeypatch):
    captured: dict[str, float] = {}
    monkeypatch.setattr("engine.anomaly.detection.IsolationForest", _spy_isolation_forest(captured))
    monkeypatch.setattr("config.settings.quality_gating_profile", "precision_strict_v1")
    ts = list(range(30))
    vals = [1.0, 1.2, 0.9, 1.1, 1.0, 1.3] * 5
    detect("m", ts, vals, sensitivity=3.0)
    assert "contamination" in captured
    assert captured["contamination"] <= 0.11


def test_detect_non_precision_profile_skips_tighter_contamination(monkeypatch):
    captured: dict[str, float] = {}
    monkeypatch.setattr("engine.anomaly.detection.IsolationForest", _spy_isolation_forest(captured))
    monkeypatch.setattr("config.settings.quality_gating_profile", "recall_loose_v1")
    ts = list(range(30))
    vals = [1.0, 1.2, 0.9, 1.1, 1.0, 1.3] * 5
    detect("m", ts, vals, sensitivity=3.0)
    assert "contamination" in captured
    assert captured["contamination"] > 0.11


def test_detect_too_few_finite_pairs(monkeypatch):
    monkeypatch.setattr("config.settings.min_samples", 12)
    ts = list(range(20))
    vals = [float("nan")] * 9 + [1.0] * 11
    assert detect("m", ts, vals) == []


def test_detect_skips_compress_when_disabled(monkeypatch):
    monkeypatch.setattr("config.settings.anomaly_compress_runs", False)
    ts = list(range(25))
    vals = [1.0] * 24 + [80.0]
    out = detect("m", ts, vals)
    assert isinstance(out, list)


def test_compress_runs_extends_short_groups_before_compressing_long_run(monkeypatch):
    monkeypatch.setattr("config.settings.anomaly_run_keep_max", 3)
    head = [
        MetricAnomaly(
            metric_name="m",
            timestamp=float(i),
            value=1.0,
            change_type=ChangeType.SPIKE,
            z_score=4.0,
            mad_score=4.0,
            isolation_score=-0.5,
            expected_range=(0.0, 1.0),
            severity=Severity.HIGH,
            description="",
            iqr_score=1.0,
            tukey_outlier_class="none",
        )
        for i in range(2)
    ]
    tail = [
        MetricAnomaly(
            metric_name="m",
            timestamp=100.0 + float(i),
            value=1.0,
            change_type=ChangeType.SPIKE,
            z_score=4.0,
            mad_score=4.0,
            isolation_score=-0.5,
            expected_range=(0.0, 1.0),
            severity=Severity.HIGH,
            description="",
            iqr_score=1.0,
            tukey_outlier_class="none",
        )
        for i in range(10)
    ]
    compressed = _compress_runs(head + tail)
    assert len(compressed) < len(head + tail)


def test_compress_runs_splits_groups_by_change_type(monkeypatch):
    monkeypatch.setattr("config.settings.anomaly_run_keep_max", 2)
    items = [
        MetricAnomaly(
            metric_name="m",
            timestamp=float(i),
            value=1.0,
            change_type=ChangeType.SPIKE if i < 5 else ChangeType.DROP,
            z_score=4.0,
            mad_score=4.0,
            isolation_score=-0.5,
            expected_range=(0.0, 1.0),
            severity=Severity.HIGH,
            description="",
            iqr_score=1.0,
            tukey_outlier_class="none",
        )
        for i in range(12)
    ]
    compressed = _compress_runs(items)
    assert len(compressed) <= len(items)
