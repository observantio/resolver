"""
Test coverage for helper functions related to filtering and processing edges in the analysis pipeline.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
"""

from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace

import pytest

from api.requests import AnalyzeRequest
from api.responses import LogBurst, LogPattern, MetricAnomaly, RootCause as RootCauseModel
from engine.analyze import filters
from engine.analyze import helpers
from engine.changepoint import ChangePoint
from engine.enums import ChangeType, Severity, Signal


def _anomaly(metric: str, ts: float, z: float, severity: Severity = Severity.MEDIUM) -> MetricAnomaly:
    return MetricAnomaly(
        metric_name=metric,
        timestamp=ts,
        value=1.0,
        change_type=ChangeType.SPIKE,
        z_score=z,
        mad_score=z / 2.0,
        isolation_score=-0.1,
        expected_range=(0.0, 2.0),
        severity=severity,
        description="x",
    )


def _cp(metric: str, ts: float, magnitude: float) -> ChangePoint:
    return ChangePoint(
        index=0,
        timestamp=ts,
        value_before=1.0,
        value_after=2.0,
        magnitude=magnitude,
        change_type=ChangeType.SHIFT,
        metric_name=metric,
    )


def test_filter_helpers_cover_all_guard_paths():
    assert filters.normalize_services([" A ", "", "b", "a"]) == {"a", "b"}

    assert filters.result_matches_services({"metric": {"service": "svc"}}, set()) is True
    assert filters.result_matches_services([], {"svc"}) is False
    assert filters.result_matches_services({"metric": []}, {"svc"}) is False
    assert filters.result_matches_services({"metric": {"service_name": "svc"}}, {"svc"}) is True
    assert filters.result_matches_services({"metric": {"job": "x"}}, {"svc"}) is False

    response = {"data": {"result": [{"metric": {"service": "svc"}}, {"metric": {"service": "other"}}]}}
    assert filters.filter_metric_response_by_services(response, set()) is response
    assert filters.filter_metric_response_by_services("bad", {"svc"}) == "bad"
    assert filters.filter_metric_response_by_services({"data": []}, {"svc"}) == {"data": []}
    assert filters.filter_metric_response_by_services({"data": {"result": "bad"}}, {"svc"}) == {
        "data": {"result": "bad"}
    }
    unchanged = {"data": {"result": [{"metric": {"service": "svc"}}]}}
    assert filters.filter_metric_response_by_services(unchanged, {"svc"}) is unchanged
    filtered = filters.filter_metric_response_by_services(response, {"svc"})
    assert filtered["data"]["result"] == [{"metric": {"service": "svc"}}]
    assert response["data"]["result"][1]["metric"]["service"] == "other"


@dataclass
class _DataclassCause:
    hypothesis: str
    confidence: object
    evidence: list[object]
    contributing_signals: list[object]
    recommended_action: str
    severity: object


def test_root_cause_conversion_registry_and_dedupe_edges():
    from engine.rca.hypothesis import RootCause

    rc_obj = helpers._to_root_cause_model(
        _DataclassCause(
            hypothesis="d1",
            confidence="not-a-number",
            evidence=[],
            contributing_signals=[Signal.LOGS, "trace-x", "event", "deploy", "unknown"],
            recommended_action="act",
            severity="low",
        )
    )
    assert rc_obj.confidence == 0.0
    assert Signal.LOGS in rc_obj.contributing_signals
    assert Signal.TRACES in rc_obj.contributing_signals
    assert Signal.EVENTS in rc_obj.contributing_signals

    rc_dict = helpers._to_root_cause_model(
        {
            "hypothesis": "d2",
            "confidence": "2.5",
            "evidence": [],
            "contributing_signals": ["metrics"],
            "recommended_action": "act",
            "severity": "low",
        }
    )
    assert rc_dict.confidence == 1.0

    rc_dataclass = RootCause(
        hypothesis="d3",
        confidence=0.2,
        severity=Severity.LOW,
        category="unknown",
        evidence=[],
        contributing_signals=["metrics"],
        affected_services=[],
        recommended_action="act",
    )
    rc_passthrough = helpers._to_root_cause_model(rc_dataclass)
    assert rc_passthrough.hypothesis == "d3"

    reg = helpers._build_compat_registry(
        [
            {
                "service": "svc",
                "timestamp": 1.0,
                "version": "v1",
                "metadata": ["not-a-dict"],
            }
        ]
    )
    assert len(reg.list_all()) == 1

    assert helpers._trim_to_len([1.0, 2.0], 2) == [1.0, 2.0]
    assert helpers._trim_to_len([1.0, 2.0], 1) == [1.0]

    deduped = helpers._dedupe_metric_anomalies(
        [
            _anomaly("m", 10.4, 1.0, Severity.LOW),
            _anomaly("m", 10.49, 2.0, Severity.LOW),
            _anomaly("m", 10.49, 1.0, Severity.HIGH),
        ]
    )
    assert len(deduped) == 1
    assert deduped[0].severity == Severity.HIGH

    cp_deduped = helpers._dedupe_change_points([_cp("m", 10.49, 1.0), _cp("m", 10.4, 2.0)])
    assert len(cp_deduped) == 1
    assert cp_deduped[0].magnitude == 2.0

    items = [
        SimpleNamespace(metric_name="a", severity=Severity.LOW, degradation_rate=0.1),
        SimpleNamespace(metric_name="a", severity=Severity.LOW, degradation_rate=0.2),
        SimpleNamespace(metric_name="b", severity=Severity.HIGH, slope_per_second=0.1),
    ]
    by_metric = helpers._dedupe_by_metric_with_severity(items)
    assert {i.metric_name for i in by_metric} == {"a", "b"}


def test_signal_scoring_and_periodic_detection_branches(monkeypatch):
    rc = RootCauseModel(
        hypothesis="h",
        confidence=0.7,
        evidence=[],
        contributing_signals=[Signal.METRICS, Signal.LOGS, Signal.TRACES, Signal.EVENTS],
        recommended_action="act",
        severity=Severity.LOW,
    )
    assert helpers._signal_key("metric.cpu") == Signal.METRICS.value
    assert helpers._signal_key("log.xyz") == Signal.LOGS.value
    assert helpers._signal_key("trace.xyz") == Signal.TRACES.value
    assert helpers._signal_key("event.xyz") == Signal.EVENTS.value
    assert helpers._signal_key("other") == "other"
    assert helpers._root_cause_signal_count(rc) >= 4
    assert "corroborating" in helpers._root_cause_corroboration_summary(rc)
    rc_empty = RootCauseModel(
        hypothesis="h2",
        confidence=0.1,
        evidence=[],
        contributing_signals=[],
        recommended_action="act",
        severity=Severity.LOW,
    )
    assert helpers._root_cause_corroboration_summary(rc_empty) == "single-signal evidence"

    ranked = SimpleNamespace(
        ml_score="0.2",
        final_score=float("inf"),
        feature_importance={"ok": "0.4", "bad": object(), "nan": float("nan")},
    )
    components = helpers._build_selection_score_components(ranked, rc)
    assert components["rule_confidence"] == 0.7
    assert components["ml_score"] == 0.2
    assert "final_score" not in components
    assert components["feature_importance:ok"] == 0.4

    assert helpers._compute_anomaly_density([], 3600.0) == {}
    density = helpers._compute_anomaly_density([_anomaly("m", 1.0, 1.0)], 30.0)
    assert density["m"] > 0.0

    monkeypatch.setattr(helpers.settings, "quality_gating_profile", "precision_strict_v1")
    assert helpers._is_precision_profile() is True
    monkeypatch.setattr(helpers.settings, "quality_gating_profile", "recall_v1")
    assert helpers._is_precision_profile() is False

    assert helpers._safe_float(object()) is None
    assert helpers._safe_float("bad") is None
    assert helpers._safe_float(float("nan")) is None
    assert helpers._safe_float("2.5") == 2.5

    assert helpers._is_strongly_periodic_log_bursts([]) is False
    non_periodic = [
        LogBurst(window_start=v, window_end=v + 5.0, rate_per_second=1.0, baseline_rate=0.1, ratio=10, severity=Severity.LOW)
        for v in [1.0, 2.0, 2.0, 3.0]
    ]
    assert helpers._is_strongly_periodic_log_bursts(non_periodic) is False


def test_apply_precision_quality_gates_non_precision_and_empty_filtered(monkeypatch):
    monkeypatch.setattr(helpers.settings, "quality_gating_profile", "precision_strict_v1")
    monkeypatch.setattr(helpers.settings, "quality_max_anomaly_density_per_metric_per_hour", 0.0)
    monkeypatch.setattr(helpers.settings, "quality_max_change_point_density_per_metric_per_hour", 0.0)
    monkeypatch.setattr(helpers.settings, "quality_min_corroboration_signals", 2)
    monkeypatch.setattr(helpers.settings, "quality_max_root_causes_without_multisignal", 1)
    monkeypatch.setattr(helpers.settings, "rca_min_confidence_display", 0.1)

    causes = [
        RootCauseModel(
            hypothesis="a",
            confidence=0.01,
            evidence=[],
            contributing_signals=[Signal.METRICS],
            recommended_action="act",
            severity=Severity.LOW,
        ),
        RootCauseModel(
            hypothesis="b",
            confidence=0.02,
            evidence=[],
            contributing_signals=[Signal.LOGS],
            recommended_action="act",
            severity=Severity.LOW,
        ),
    ]
    ranked = [SimpleNamespace(root_cause=SimpleNamespace(hypothesis="x"), final_score=0.2)]
    suppression_counts: dict[str, int] = {}
    warnings: list[str] = []
    _, _, causes_after, ranked_after, quality = helpers._apply_precision_quality_gates(
        metric_anomalies=[_anomaly("m", 1.0, 1.0)],
        change_points=[_cp("m", 1.0, 1.0)],
        root_causes=causes,
        ranked_causes=ranked,
        duration_seconds=3600.0,
        suppression_counts=suppression_counts,
        warnings=warnings,
    )
    assert len(causes_after) == 1
    assert ranked_after == []
    assert quality.gating_profile == "precision_strict_v1"

    monkeypatch.setattr(helpers.settings, "quality_gating_profile", "recall")
    _, _, _, _, quality_non_precision = helpers._apply_precision_quality_gates(
        metric_anomalies=[_anomaly("m", 1.0, 1.0)],
        change_points=[_cp("m", 1.0, 1.0)],
        root_causes=causes,
        ranked_causes=ranked,
        duration_seconds=120.0,
        suppression_counts={},
        warnings=[],
    )
    assert quality_non_precision.gating_profile == "recall"


@pytest.mark.asyncio
async def test_process_metric_series_and_metrics_pipeline_branches(monkeypatch):
    async def _raise_compute(*_args, **_kwargs):
        raise RuntimeError("no-store")

    called = {"baseline_fallback": 0, "forecast": 0, "degradation": 0}

    monkeypatch.setattr(helpers.baseline_store, "compute_and_persist", _raise_compute)
    monkeypatch.setattr(helpers, "baseline_compute", lambda *_a, **_k: called.__setitem__("baseline_fallback", 1))
    monkeypatch.setattr(helpers.anomaly, "detect", lambda *_a, **_k: [_anomaly("m", 1.0, 1.0)])

    def _legacy_cp(ts, vals, sigma):
        return [_cp("m", 1.0, sigma)]

    monkeypatch.setattr(helpers, "changepoint_detect", _legacy_cp)
    monkeypatch.setattr(helpers.settings, "cusum_threshold_sigma", 2.0)
    monkeypatch.setattr(helpers.settings, "analyzer_forecast_min_window_seconds", 10.0)
    monkeypatch.setattr(helpers.settings, "analyzer_degradation_min_window_seconds", 10.0)
    monkeypatch.setattr(helpers, "forecast", lambda *_a, **_k: called.__setitem__("forecast", 1) or SimpleNamespace())
    monkeypatch.setattr(
        helpers,
        "analyze_degradation",
        lambda *_a, **_k: called.__setitem__("degradation", 1) or SimpleNamespace(),
    )

    req = AnalyzeRequest(tenant_id="t", start=1, end=2, step="15s")
    key = next(iter(helpers.FORECAST_THRESHOLDS.keys()))
    anomalies, change_points, fc, deg = await helpers._process_one_metric_series(
        req=req,
        query_string=key,
        metric_name="m",
        ts=[1.0, 2.0, 3.0],
        vals=[1.0, 2.0, 3.0],
        z_threshold=0.0,
        analysis_window_seconds=20.0,
    )
    assert anomalies
    assert change_points
    assert fc is not None
    assert deg is not None
    assert called["baseline_fallback"] == 1
    assert called["forecast"] == 1
    assert called["degradation"] == 1

    # Exercise _process_metrics branches: service filtering, row None, exception result and fc/deg falsey.
    async def _fake_fetch_metrics(*_args, **_kwargs):
        return [("q1", {"data": {"result": [1]}}), ("q2", {"data": {"result": [2]}})]

    monkeypatch.setattr(helpers, "fetch_metrics", _fake_fetch_metrics)
    monkeypatch.setattr(helpers, "_normalize_services", lambda _s: {"svc"})
    monkeypatch.setattr(helpers, "_filter_metric_response_by_services", lambda resp, _svc: resp if resp != {"bad": 1} else [])

    def _iter_series(resp, query_hint=None):
        if query_hint == "q1":
            return iter([("m1", [1.0, 2.0], [10.0, 11.0])])
        return iter([("m2", [1.0, 2.0], [20.0, 21.0])])

    monkeypatch.setattr(helpers.anomaly, "iter_series", _iter_series)
    monkeypatch.setattr(helpers, "compute_series_distribution_stats", lambda sk, mn, vals: None if mn == "m1" else SimpleNamespace(series_key=sk))

    async def _process_one(req, query_string, metric_name, ts, vals, z_threshold, analysis_window_seconds):
        if metric_name == "m1":
            raise RuntimeError("boom")
        return ([_anomaly(metric_name, 1.0, 1.0)], [_cp(metric_name, 1.0, 1.0)], None, None)

    monkeypatch.setattr(helpers, "_process_one_metric_series", _process_one)

    m_anoms, cps, fcs, degs, series_map, dist = await helpers._process_metrics(
        provider=SimpleNamespace(),
        req=AnalyzeRequest(tenant_id="t", start=1, end=2, step="15s", services=["svc"]),
        all_metric_queries=["q1", "q2"],
        z_threshold=1.0,
        analysis_window_seconds=20.0,
    )
    assert len(m_anoms) == 1
    assert len(cps) == 1
    assert fcs == []
    assert degs == []
    assert "q1::m1" in series_map
    assert len(dist) == 1


def test_slo_pairs_and_granger_selection_edges(monkeypatch):
    def _iter_series(payload, query_hint=None):
        if query_hint == helpers.SLO_ERROR_QUERY:
            return iter([("e", [1.0, 2.0], [0.0, 1.0]), ("e2", [3.0], [2.0])])
        return iter([("t", [1.0], [10.0])])

    monkeypatch.setattr(helpers.anomaly, "iter_series", _iter_series)
    warnings: list[str] = []
    pairs = helpers._slo_series_pairs({"data": {}}, {"data": {}}, warnings)
    assert len(pairs) == 1
    assert warnings

    monkeypatch.setattr(helpers.settings, "analyzer_granger_min_samples", 5)
    monkeypatch.setattr(helpers.settings, "analyzer_granger_max_series", 2)
    selected = helpers._select_granger_series(
        {
            "const": [1.0] * 10,
            "short": [1.0, 2.0],
            "finite": [1.0, 2.0, 4.0, 8.0, 16.0],
            "finite2": [2.0, 3.0, 5.0, 7.0, 11.0],
            "nanmix": [1.0, float("nan"), 2.0, 3.0, 4.0, 5.0],
        }
    )
    assert "const" not in selected
    assert "short" not in selected
    assert len(selected) <= 2


def test_remaining_small_helper_branches():
    # _to_root_cause_model: non-list signals branch, confidence fallback branch, direct model_validate branch.
    with pytest.raises(Exception):
        helpers._to_root_cause_model(
            {
                "hypothesis": "h",
                "confidence": object(),
                "evidence": [],
                "contributing_signals": "metrics",
                "recommended_action": "act",
                "severity": "low",
            }
        )
    rc_model = RootCauseModel(
        hypothesis="h2",
        confidence=0.3,
        evidence=[],
        contributing_signals=[Signal.METRICS],
        recommended_action="act",
        severity=Severity.LOW,
    )
    rc_passthrough = helpers._to_root_cause_model(rc_model)
    assert rc_passthrough.hypothesis == "h2"

    # _dedupe_metric_anomalies: equal-severity lower z-score keeps first.
    kept = helpers._dedupe_metric_anomalies(
        [
            _anomaly("m", 1.0, 5.0),
            _anomaly("m", 1.0, 4.0),  # equal severity, lower signal -> keep current (branch fallthrough)
            _anomaly("n", 2.0, 1.0),
        ]
    )
    assert len(kept) == 2
    by_metric = {item.metric_name: item for item in kept}
    assert by_metric["m"].z_score == 5.0

    # _dedupe_by_metric_with_severity: higher severity replacement path.
    deduped = helpers._dedupe_by_metric_with_severity(
        [
            SimpleNamespace(metric_name="x", severity=Severity.LOW, degradation_rate=0.1),
            SimpleNamespace(metric_name="x", severity=Severity.HIGH, degradation_rate=0.05),
            SimpleNamespace(metric_name="x", severity=Severity.HIGH, degradation_rate=0.01),
        ]
    )
    assert deduped[0].severity == Severity.HIGH

    # _build_selection_score_components: None skip, conversion errors, non-dict importances.
    root = RootCauseModel(
        hypothesis="h3",
        confidence=0.4,
        evidence=[],
        contributing_signals=[Signal.LOGS],
        recommended_action="act",
        severity=Severity.LOW,
    )
    comps = helpers._build_selection_score_components(
        SimpleNamespace(ml_score=None, final_score="bad", feature_importance=None),
        root,
    )
    assert "ml_score" not in comps
    assert "final_score" not in comps
    comps2 = helpers._build_selection_score_components(
        SimpleNamespace(ml_score=0.2, final_score=0.3, feature_importance={"bad": "oops"}),
        root,
    )
    assert "feature_importance:bad" not in comps2


def test_periodic_and_log_burst_filter_remaining_branches(monkeypatch):
    # starts<4 after filtering invalid values
    bad_starts = [
        SimpleNamespace(window_start=1.0),
        SimpleNamespace(window_start=2.0),
        SimpleNamespace(window_start="nan"),
        SimpleNamespace(window_start=None),
    ]
    assert helpers._is_strongly_periodic_log_bursts(bad_starts) is False

    # median out of accepted range (>180)
    wide = [
        LogBurst(window_start=v, window_end=v + 5.0, rate_per_second=1.0, baseline_rate=0.1, ratio=10, severity=Severity.LOW)
        for v in [0.0, 500.0, 1000.0, 1500.0]
    ]
    assert helpers._is_strongly_periodic_log_bursts(wide) is False

    # high coefficient of variation
    noisy = [
        LogBurst(window_start=v, window_end=v + 5.0, rate_per_second=1.0, baseline_rate=0.1, ratio=10, severity=Severity.LOW)
        for v in [0.0, 20.0, 200.0, 230.0, 500.0]
    ]
    assert helpers._is_strongly_periodic_log_bursts(noisy) is False

    bursts = [
        LogBurst(window_start=1000.0 + i * 60.0, window_end=1005.0 + i * 60.0, rate_per_second=1.0, baseline_rate=0.1, ratio=10, severity=Severity.LOW)
        for i in range(4)
    ]
    patterns = [LogPattern(pattern="x", count=4, first_seen=1.0, last_seen=2.0, rate_per_minute=1.0, entropy=0.1, severity=Severity.LOW, sample="x")]
    suppression_counts: dict[str, int] = {}
    warnings: list[str] = []

    monkeypatch.setattr(helpers.settings, "quality_gating_profile", "recall")
    assert helpers._filter_log_bursts_for_precision_rca(
        log_bursts=bursts, log_patterns=patterns, suppression_counts=suppression_counts, warnings=warnings
    ) == bursts

    monkeypatch.setattr(helpers.settings, "quality_gating_profile", "precision_strict_v1")
    assert helpers._filter_log_bursts_for_precision_rca(
        log_bursts=bursts, log_patterns=[], suppression_counts=suppression_counts, warnings=warnings
    ) == bursts

    # non-periodic path under precision + low severity patterns
    assert helpers._filter_log_bursts_for_precision_rca(
        log_bursts=noisy, log_patterns=patterns, suppression_counts=suppression_counts, warnings=warnings
    ) == noisy


@pytest.mark.asyncio
async def test_process_metrics_and_slo_remaining_branches(monkeypatch):
    monkeypatch.setattr(helpers.settings, "analyzer_degradation_min_window_seconds", 99999.0)
    monkeypatch.setattr(helpers.settings, "analyzer_forecast_min_window_seconds", 99999.0)

    async def _ok_compute(*_args, **_kwargs):
        return SimpleNamespace()

    monkeypatch.setattr(helpers.baseline_store, "compute_and_persist", _ok_compute)
    monkeypatch.setattr(helpers.anomaly, "detect", lambda *_a, **_k: [])
    monkeypatch.setattr(helpers, "changepoint_detect", lambda *_a, **_k: [])
    monkeypatch.setattr(helpers, "forecast", lambda *_a, **_k: SimpleNamespace())
    monkeypatch.setattr(helpers, "analyze_degradation", lambda *_a, **_k: SimpleNamespace())

    req = AnalyzeRequest(tenant_id="t", start=1, end=2, step="15s")
    _, _, _, deg = await helpers._process_one_metric_series(
        req=req,
        query_string="no-threshold-key",
        metric_name="m",
        ts=[1.0, 2.0],
        vals=[1.0, 2.0],
        z_threshold=1.0,
        analysis_window_seconds=10.0,
    )
    assert deg is None

    async def _fake_fetch(*_args, **_kwargs):
        return [("q1", {"data": {"result": [1]}})]

    monkeypatch.setattr(helpers, "fetch_metrics", _fake_fetch)

    def _iter_series(_resp, query_hint=None):
        return iter([("m", [1.0], [1.0])])

    monkeypatch.setattr(helpers.anomaly, "iter_series", _iter_series)
    monkeypatch.setattr(helpers, "compute_series_distribution_stats", lambda *_a, **_k: SimpleNamespace(series_key="s"))

    async def _process_one(*_args, **_kwargs):
        return ([], [], SimpleNamespace(name="fc"), None)

    monkeypatch.setattr(helpers, "_process_one_metric_series", _process_one)
    _, _, fcs, _degs, _series_map, _dist = await helpers._process_metrics(
        provider=SimpleNamespace(),
        req=AnalyzeRequest(tenant_id="t", start=1, end=2, step="15s", services=[]),
        all_metric_queries=["q1"],
        z_threshold=1.0,
        analysis_window_seconds=10.0,
    )
    assert len(fcs) == 1

    # _slo_series_pairs: trimmed to empty pair should skip append (branch 743->734).
    def _iter_series_slo(_payload, query_hint=None):
        if query_hint == helpers.SLO_ERROR_QUERY:
            return iter([("e", [], [])])
        return iter([("t", [1.0], [1.0])])

    monkeypatch.setattr(helpers.anomaly, "iter_series", _iter_series_slo)
    warnings: list[str] = []
    pairs = helpers._slo_series_pairs({"data": {}}, {"data": {}}, warnings)
    assert pairs == []
