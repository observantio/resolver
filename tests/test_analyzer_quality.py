"""
Test Analyzer Quality and output formatting to ensure results are correctly processed and limited before returning to
clients.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
"""

from __future__ import annotations

from types import SimpleNamespace

from api.responses import LogBurst, LogPattern, MetricAnomaly
from api.responses import RootCause as RootCauseModel
from engine.analyzer import (
    _apply_precision_quality_gates,
    _build_log_query,
    _filter_log_bursts_for_precision_rca,
    _limit_analyzer_output,
    _select_granger_series,
    _to_root_cause_model,
)
from engine.causal.granger import GrangerResult
from engine.changepoint import ChangePoint
from engine.enums import ChangeType, RcaCategory, Severity, Signal
from engine.ml.clustering import AnomalyCluster
from engine.ml.ranking import RankedCause
from engine.rca.hypothesis import RootCause


def _anomaly(idx: int, severity: Severity) -> MetricAnomaly:
    return MetricAnomaly(
        metric_name=f"m{idx}",
        timestamp=1000.0 + idx,
        value=float(idx),
        change_type=ChangeType.SPIKE,
        z_score=float(idx),
        mad_score=float(idx) / 2.0,
        isolation_score=0.5,
        expected_range=(0.0, 10.0),
        severity=severity,
        description=f"m{idx} anomaly",
    )


def _ranked(idx: int, final_score: float) -> RankedCause:
    rc = RootCause(
        hypothesis=f"cause-{idx}",
        confidence=min(1.0, max(0.0, final_score)),
        severity=Severity.MEDIUM,
        category=RcaCategory.UNKNOWN,
        evidence=[],
        contributing_signals=["metrics"],
        affected_services=[],
        recommended_action="investigate",
    )
    return RankedCause(root_cause=rc, ml_score=final_score, final_score=final_score, feature_importance={})


def test_to_root_cause_model_clamps_invalid_confidence():
    rc = _to_root_cause_model(
        {
            "hypothesis": "test",
            "confidence": "nan",
            "evidence": [],
            "contributing_signals": ["metrics", "log:burst"],
            "recommended_action": "act",
            "severity": "low",
        }
    )
    assert isinstance(rc, RootCauseModel)
    assert rc.confidence == 0.0
    assert Signal.METRICS in rc.contributing_signals
    assert Signal.LOGS in rc.contributing_signals


def test_limit_analyzer_output_caps_noise_lists():
    anomalies = [_anomaly(i, Severity.CRITICAL if i % 2 else Severity.LOW) for i in range(500)]
    change_points = [
        ChangePoint(
            index=i,
            timestamp=10.0 + i,
            value_before=1.0,
            value_after=2.0,
            magnitude=float(i),
            change_type=ChangeType.SHIFT,
            metric_name=f"c{i}",
        )
        for i in range(400)
    ]
    root_causes = [
        RootCauseModel(
            hypothesis=f"h{i}",
            confidence=min(1.0, i / 20.0),
            evidence=[],
            contributing_signals=[Signal.METRICS],
            recommended_action="x",
            severity=Severity.LOW,
        )
        for i in range(40)
    ]
    ranked = [_ranked(i, i / 40.0) for i in range(40)]
    clusters = [
        AnomalyCluster(
            cluster_id=i,
            members=anomalies[: (i + 1)],
            centroid_timestamp=1000.0,
            centroid_value=1.0,
            metric_names=["m"],
            size=i + 1,
        )
        for i in range(50)
    ]
    granger = [
        GrangerResult(
            cause_metric=f"a{i}",
            effect_metric=f"b{i}",
            max_lag=2,
            f_statistic=1.0,
            p_value=0.01,
            is_causal=True,
            strength=i / 50.0,
        )
        for i in range(220)
    ]
    warnings: list[str] = []

    (
        anomalies_limited,
        cps_limited,
        causes_limited,
        ranked_limited,
        clusters_limited,
        granger_limited,
    ) = _limit_analyzer_output(
        metric_anomalies=anomalies,
        change_points=change_points,
        root_causes=root_causes,
        ranked_causes=ranked,
        anomaly_clusters=clusters,
        granger_results=granger,
        warnings=warnings,
    )

    assert len(anomalies_limited) <= 250
    assert len(cps_limited) <= 200
    assert len(causes_limited) <= 15
    assert len(ranked_limited) <= 15
    assert len(clusters_limited) <= 30
    assert len(granger_limited) <= 100
    assert warnings


def test_select_granger_series_filters_constant_and_short_series(monkeypatch):
    monkeypatch.setattr("config.settings.analyzer_granger_min_samples", 5)
    monkeypatch.setattr("config.settings.analyzer_granger_max_series", 2)
    selected = _select_granger_series(
        {
            "const": [1.0] * 10,
            "short": [1.0, 2.0, 3.0],
            "v1": [1.0, 3.0, 2.0, 5.0, 4.0, 7.0],
            "v2": [2.0, 5.0, 3.0, 9.0, 4.0, 12.0],
            "v3": [1.0, 8.0, 2.0, 7.0, 3.0, 6.0],
        }
    )
    assert "const" not in selected
    assert "short" not in selected
    assert len(selected) <= 2


def test_to_root_cause_model_includes_additive_defaults():
    rc = _to_root_cause_model(
        {
            "hypothesis": "test",
            "confidence": 0.4,
            "evidence": [],
            "contributing_signals": ["metrics"],
            "recommended_action": "act",
            "severity": "low",
        }
    )
    assert rc.corroboration_summary is None
    assert rc.suppression_diagnostics == {}
    assert rc.selection_score_components == {}


def test_apply_precision_quality_gates_enforces_density_and_root_cause_filters(monkeypatch):
    monkeypatch.setattr("config.settings.quality_gating_profile", "precision_strict_v1")
    monkeypatch.setattr("config.settings.quality_max_anomaly_density_per_metric_per_hour", 1.0)
    monkeypatch.setattr("config.settings.quality_max_root_causes_without_multisignal", 1)
    monkeypatch.setattr("config.settings.quality_min_corroboration_signals", 2)
    monkeypatch.setattr("config.settings.quality_confidence_calibration_version", "calib_test")
    monkeypatch.setattr("config.settings.rca_min_confidence_display", 0.05)

    anomalies = [
        MetricAnomaly(
            metric_name="shared_metric",
            timestamp=float(100 + i * 10),
            value=float(i),
            change_type=ChangeType.SPIKE,
            z_score=5.0 + i,
            mad_score=4.0 + i,
            isolation_score=-0.5,
            expected_range=(0.0, 1.0),
            severity=Severity.HIGH,
            description="",
        )
        for i in range(6)
    ]
    causes = [
        RootCauseModel(
            hypothesis="low-confidence-single-signal",
            confidence=0.08,
            evidence=[],
            contributing_signals=[Signal.METRICS],
            recommended_action="inspect",
            severity=Severity.LOW,
        ),
        RootCauseModel(
            hypothesis="multi-signal-cause",
            confidence=0.75,
            evidence=[],
            contributing_signals=[Signal.METRICS, Signal.LOGS],
            recommended_action="rollback",
            severity=Severity.HIGH,
        ),
    ]
    ranked = [
        SimpleNamespace(root_cause=SimpleNamespace(hypothesis="low-confidence-single-signal"), final_score=0.1),
        SimpleNamespace(root_cause=SimpleNamespace(hypothesis="multi-signal-cause"), final_score=0.8),
    ]
    change_points = [
        ChangePoint(
            index=i,
            timestamp=float(100 + i * 10),
            value_before=1.0,
            value_after=2.0,
            magnitude=float(i + 1),
            change_type=ChangeType.SHIFT,
            metric_name="shared_metric",
        )
        for i in range(6)
    ]
    warnings: list[str] = []
    suppression_counts: dict[str, int] = {}

    anomalies_after, change_points_after, causes_after, ranked_after, quality = _apply_precision_quality_gates(
        metric_anomalies=anomalies,
        change_points=change_points,
        root_causes=causes,
        ranked_causes=ranked,
        duration_seconds=3600.0,
        suppression_counts=suppression_counts,
        warnings=warnings,
    )

    assert len(anomalies_after) == 1
    assert len(change_points_after) == 2
    assert len(causes_after) == 1
    assert causes_after[0].hypothesis == "multi-signal-cause"
    assert causes_after[0].corroboration_summary
    assert causes_after[0].suppression_diagnostics.get("meets_min_corroboration_signals") is True
    assert len(ranked_after) == 1
    assert quality.gating_profile == "precision_strict_v1"
    assert quality.confidence_calibration_version == "calib_test"
    assert quality.suppression_counts.get("density_suppressed_metric_anomalies", 0) >= 1
    assert quality.suppression_counts.get("density_suppressed_change_points", 0) >= 1
    assert quality.suppression_counts.get("low_confidence_root_causes", 0) >= 1


def test_build_log_query_defaults_to_global_selector():
    assert _build_log_query([], None) == '{service_name=~".+"}'
    assert _build_log_query(None, None) == '{service_name=~".+"}'


def test_build_log_query_services_use_service_name_label():
    query = _build_log_query(["payments"], None)
    assert query == '{service_name=~"payments"}'


def test_filter_log_bursts_for_precision_rca_suppresses_periodic_low_signal(monkeypatch):
    monkeypatch.setattr("config.settings.quality_gating_profile", "precision_strict_v1")
    bursts = [
        LogBurst(
            window_start=1000.0 + (i * 60.0),
            window_end=1010.0 + (i * 60.0),
            rate_per_second=0.5,
            baseline_rate=0.1,
            ratio=5.0,
            severity=Severity.HIGH,
        )
        for i in range(6)
    ]
    patterns = [
        LogPattern(
            pattern="background saving terminated with success",
            count=6,
            first_seen=1000.0,
            last_seen=1300.0,
            rate_per_minute=1.0,
            entropy=0.1,
            severity=Severity.LOW,
            sample="Background saving terminated with success",
        )
    ]
    suppression_counts: dict[str, int] = {}
    warnings: list[str] = []
    filtered = _filter_log_bursts_for_precision_rca(
        log_bursts=bursts,
        log_patterns=patterns,
        suppression_counts=suppression_counts,
        warnings=warnings,
    )
    assert filtered == []
    assert suppression_counts.get("low_signal_periodic_log_bursts") == len(bursts)
    assert any("periodic low-severity" in warning for warning in warnings)


def test_filter_log_bursts_for_precision_rca_keeps_high_signal(monkeypatch):
    monkeypatch.setattr("config.settings.quality_gating_profile", "precision_strict_v1")
    bursts = [
        LogBurst(
            window_start=1000.0 + (i * 60.0),
            window_end=1010.0 + (i * 60.0),
            rate_per_second=0.5,
            baseline_rate=0.1,
            ratio=5.0,
            severity=Severity.HIGH,
        )
        for i in range(4)
    ]
    patterns = [
        LogPattern(
            pattern="timeout while calling dependency",
            count=4,
            first_seen=1000.0,
            last_seen=1180.0,
            rate_per_minute=1.0,
            entropy=0.2,
            severity=Severity.HIGH,
            sample="timeout while calling dependency",
        )
    ]
    suppression_counts: dict[str, int] = {}
    warnings: list[str] = []
    filtered = _filter_log_bursts_for_precision_rca(
        log_bursts=bursts,
        log_patterns=patterns,
        suppression_counts=suppression_counts,
        warnings=warnings,
    )
    assert len(filtered) == len(bursts)
    assert suppression_counts == {}
    assert warnings == []
