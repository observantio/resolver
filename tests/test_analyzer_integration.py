"""
Test AnalyzerService integration with the core analysis engine and data providers.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
"""

import asyncio
from types import SimpleNamespace

import pytest

from api.requests import AnalyzeRequest
from api.responses import MetricAnomaly
from engine import analyzer
from engine.baseline.compute import Baseline
from engine.enums import ChangeType, RcaCategory, Severity
from engine.ml.ranking import RankedCause
from engine.rca.hypothesis import RootCause


def _metric_result(name: str, values: list[float], service: str = "payment-service") -> dict:
    ts = list(range(1, len(values) + 1))
    return {
        "status": "success",
        "data": {
            "result": [
                {
                    "metric": {
                        "__name__": name,
                        "service": service,
                        "service_name": service,
                    },
                    "values": [[t, str(v)] for t, v in zip(ts, values)],
                }
            ]
        },
    }


class DummyProvider:
    async def query_logs(self, query: str, start: int, end: int, limit=None):
        dense = [(30 + i * 0.01, f"ERROR burst {i}") for i in range(100)]
        sparse = [(200 + i * 10, "normal") for i in range(20)]
        values = [[str(int(t * 1e9)), line] for t, line in dense + sparse]
        return {
            "data": {
                "result": [
                    {
                        "stream": {"service": "payment-service", "level": "error"},
                        "values": values,
                    }
                ]
            }
        }

    async def query_traces(self, filters, start: int, end: int, limit=None):
        traces = []
        for service in ("payment-service", "order-service"):
            for i in range(8):
                traces.append(
                    {
                        "rootServiceName": service,
                        "rootTraceName": f"{service}.op",
                        "durationMs": 6000.0 if i % 2 == 0 else 100.0,
                        "spanSet": {
                            "spans": [
                                {
                                    "attributes": [
                                        {"key": "status.code", "value": {"stringValue": "STATUS_CODE_ERROR" if i % 3 == 0 else "OK"}},
                                    ]
                                }
                            ]
                        },
                        "spanSets": [
                            {
                                "attributes": [
                                    {"key": "service.name", "value": {"stringValue": service}},
                                    {"key": "peer.service", "value": {"stringValue": "db"}},
                                ]
                            }
                        ],
                    }
                )
        return {"traces": traces}

    async def query_metrics(self, query: str, start: int, end: int, step: str):
        if query == analyzer.SLO_ERROR_QUERY:
            return _metric_result("slo_errors", [1.0] * 40)
        if query == analyzer.SLO_TOTAL_QUERY:
            return _metric_result("slo_total", [100.0] * 40)

        base = [1.0] * 40
        base[30] = 100.0
        return _metric_result("shared_metric", base)


class EmptyProvider:
    async def query_logs(self, query: str, start: int, end: int, limit=None):
        return {"data": {"result": []}}

    async def query_traces(self, filters, start: int, end: int, limit=None):
        return {"traces": []}

    async def query_metrics(self, query: str, start: int, end: int, step: str):
        return {"status": "success", "data": {"result": []}}


class EmptyTracesWithCountProvider(EmptyProvider):
    async def query_traces(self, filters, start: int, end: int, limit=None):
        if limit == 10001:
            return {"traces": [{"traceID": f"t{i}"} for i in range(10001)]}
        return {"traces": []}


class DummyRegistry:
    async def events_in_window(self, tenant_id: str, start: int, end: int):
        return []

    async def get_state(self, tenant_id: str):
        # return a minimal object supporting weighted_confidence used by analyzer
        class _State:
            def weighted_confidence(self, metric_score, log_score, trace_score):
                # default behaviour mirrors previous unweighted logic
                return metric_score + log_score + trace_score
        return _State()


def fake_detect(metric_name, ts, vals, sensitivity=None):
    t = float(ts[len(ts) // 2])
    v = float(vals[len(vals) // 2])
    return [
        MetricAnomaly(
            metric_name=metric_name,
            timestamp=t,
            value=v,
            change_type=ChangeType.spike,
            z_score=5.0,
            mad_score=5.0,
            isolation_score=-0.5,
            expected_range=(0.0, 1.0),
            severity=Severity.high,
            description=f"{metric_name} spike",
        )
    ]


@pytest.mark.asyncio
async def test_analyzer_run_non_empty_path_and_tenant_isolation(monkeypatch):
    monkeypatch.setattr(analyzer, "DEFAULT_METRIC_QUERIES", ["q_a", "q_b"])
    monkeypatch.setattr(analyzer, "get_registry", lambda: DummyRegistry())
    monkeypatch.setattr(analyzer.anomaly, "detect", fake_detect)
    monkeypatch.setattr(analyzer, "changepoint_detect", lambda ts, vals, threshold_sigma=None: [])
    monkeypatch.setattr(analyzer, "test_all_pairs", lambda series_map, max_lag=None, p_threshold=None: [])

    captured = {"baseline_tenants": set(), "granger_tenants": set()}

    async def fake_compute_and_persist(tenant_id, metric_name, ts, vals, z_threshold=3.0):
        captured["baseline_tenants"].add(tenant_id)
        return Baseline(mean=1.0, std=1.0, lower=0.0, upper=2.0, sample_count=len(vals))

    async def fake_save_and_merge(tenant_id, service, fresh_results):
        captured["granger_tenants"].add(tenant_id)
        return []

    monkeypatch.setattr(analyzer.baseline_store, "compute_and_persist", fake_compute_and_persist)
    monkeypatch.setattr(analyzer.granger_store, "save_and_merge", fake_save_and_merge)

    req = AnalyzeRequest(tenant_id="tenant-one", start=1, end=3600, step="15s", services=["payment-service"])
    report = await analyzer.run(DummyProvider(), req)

    assert report.tenant_id == "tenant-one"
    assert report.log_bursts
    assert report.metric_anomalies
    assert report.service_latency
    assert report.summary
    assert isinstance(report.analysis_warnings, list)
    assert report.quality is not None
    assert report.quality.gating_profile.startswith("precision")
    assert all(v <= 1.1 for v in (report.quality.anomaly_density or {}).values())
    if report.root_causes:
        assert report.root_causes[0].selection_score_components
        assert report.root_causes[0].corroboration_summary
    assert captured["baseline_tenants"] == {"tenant-one"}
    assert captured["granger_tenants"] == {"tenant-one"}
    assert report.metric_series_statistics
    assert all(getattr(row, "series_key", "") for row in report.metric_series_statistics)


@pytest.mark.asyncio
async def test_analyzer_concurrent_runs_are_consistent(monkeypatch):
    monkeypatch.setattr(analyzer, "DEFAULT_METRIC_QUERIES", ["q_a", "q_b"])
    monkeypatch.setattr(analyzer, "get_registry", lambda: DummyRegistry())
    monkeypatch.setattr(analyzer.anomaly, "detect", fake_detect)
    monkeypatch.setattr(analyzer, "changepoint_detect", lambda ts, vals, threshold_sigma=None: [])
    monkeypatch.setattr(analyzer, "test_all_pairs", lambda series_map, max_lag=None, p_threshold=None: [])

    async def fake_compute_and_persist(tenant_id, metric_name, ts, vals, z_threshold=3.0):
        return Baseline(mean=1.0, std=1.0, lower=0.0, upper=2.0, sample_count=len(vals))

    async def fake_save_and_merge(tenant_id, service, fresh_results):
        return []

    monkeypatch.setattr(analyzer.baseline_store, "compute_and_persist", fake_compute_and_persist)
    monkeypatch.setattr(analyzer.granger_store, "save_and_merge", fake_save_and_merge)

    req = AnalyzeRequest(tenant_id="tenant-perf", start=1, end=3600, step="15s", services=["payment-service"])
    reports = await asyncio.gather(*[analyzer.run(DummyProvider(), req) for _ in range(5)])

    assert len(reports) == 5
    assert all(report.tenant_id == "tenant-perf" for report in reports)
    assert all(report.metric_anomalies for report in reports)
    assert all(report.log_bursts for report in reports)
    assert all(report.service_latency for report in reports)

    baseline_shape = (
        len(reports[0].metric_anomalies),
        len(reports[0].log_bursts),
        len(reports[0].service_latency),
    )
    for report in reports[1:]:
        assert (
            len(report.metric_anomalies),
            len(report.log_bursts),
            len(report.service_latency),
        ) == baseline_shape


@pytest.mark.asyncio
async def test_analyzer_uses_causal_and_topology_graph_helpers(monkeypatch):
    monkeypatch.setattr(analyzer, "DEFAULT_METRIC_QUERIES", ["q_a", "q_b"])
    monkeypatch.setattr(analyzer, "get_registry", lambda: DummyRegistry())
    monkeypatch.setattr(analyzer.anomaly, "detect", fake_detect)
    monkeypatch.setattr(analyzer, "changepoint_detect", lambda ts, vals, threshold_sigma=None: [])
    monkeypatch.setattr(analyzer, "test_all_pairs", lambda series_map, max_lag=None, p_threshold=None: [])

    called = {"find_common_causes": 0, "critical_path": 0}
    original_find_common_causes = analyzer.CausalGraph.find_common_causes
    original_critical_path = analyzer.DependencyGraph.critical_path

    def spy_find_common_causes(self, node_a, node_b):
        called["find_common_causes"] += 1
        return original_find_common_causes(self, node_a, node_b)

    def spy_critical_path(self, source, target):
        called["critical_path"] += 1
        return original_critical_path(self, source, target)

    async def fake_compute_and_persist(tenant_id, metric_name, ts, vals, z_threshold=3.0):
        return Baseline(mean=1.0, std=1.0, lower=0.0, upper=2.0, sample_count=len(vals))

    async def fake_save_and_merge(tenant_id, service, fresh_results):
        return []

    monkeypatch.setattr(analyzer.CausalGraph, "find_common_causes", spy_find_common_causes)
    monkeypatch.setattr(analyzer.DependencyGraph, "critical_path", spy_critical_path)
    monkeypatch.setattr(analyzer.baseline_store, "compute_and_persist", fake_compute_and_persist)
    monkeypatch.setattr(analyzer.granger_store, "save_and_merge", fake_save_and_merge)

    req = AnalyzeRequest(tenant_id="tenant-helpers", start=1, end=3600, step="15s", services=["payment-service"])
    await analyzer.run(DummyProvider(), req)

    assert called["find_common_causes"] >= 1
    assert called["critical_path"] >= 1


@pytest.mark.asyncio
async def test_analyzer_empty_inputs_returns_safe_report(monkeypatch):
    monkeypatch.setattr(analyzer, "DEFAULT_METRIC_QUERIES", ["q_a"])
    monkeypatch.setattr(analyzer, "get_registry", lambda: DummyRegistry())

    req = AnalyzeRequest(tenant_id="tenant-empty", start=1, end=3600, step="15s", services=["payment-service"])
    report = await analyzer.run(EmptyProvider(), req)

    assert report.tenant_id == "tenant-empty"
    assert report.metric_anomalies == []
    assert report.log_bursts == []
    assert report.service_latency == []
    assert report.root_causes == []
    assert "No anomalies detected" in report.summary
    assert any("returned no entries" in warning or "returned no traces" in warning for warning in report.analysis_warnings)


@pytest.mark.asyncio
async def test_analyzer_trace_id_fallback_reports_10000_plus(monkeypatch):
    monkeypatch.setattr(analyzer, "DEFAULT_METRIC_QUERIES", ["q_a"])
    monkeypatch.setattr(analyzer, "get_registry", lambda: DummyRegistry())

    req = AnalyzeRequest(tenant_id="tenant-empty", start=1, end=3600, step="15s", services=["payment-service"])
    report = await analyzer.run(EmptyTracesWithCountProvider(), req)

    assert any("10000+" in warning for warning in report.analysis_warnings)


@pytest.mark.asyncio
async def test_analyzer_enforces_caps_during_run(monkeypatch):
    monkeypatch.setattr(analyzer, "DEFAULT_METRIC_QUERIES", ["q_a", "q_b", "q_c"])
    monkeypatch.setattr(analyzer, "get_registry", lambda: DummyRegistry())
    monkeypatch.setattr(analyzer.anomaly, "detect", fake_detect)
    monkeypatch.setattr(analyzer, "changepoint_detect", lambda ts, vals, threshold_sigma=None: [])
    monkeypatch.setattr(analyzer, "test_all_pairs", lambda series_map, max_lag=None, p_threshold=None: [])
    monkeypatch.setattr("config.settings.analyzer_max_metric_anomalies", 2)
    monkeypatch.setattr("config.settings.analyzer_max_root_causes", 1)
    monkeypatch.setattr("config.settings.analyzer_max_granger_pairs", 1)
    monkeypatch.setattr("config.settings.analyzer_max_clusters", 1)
    monkeypatch.setattr("config.settings.analyzer_max_change_points", 1)

    async def fake_compute_and_persist(tenant_id, metric_name, ts, vals, z_threshold=3.0):
        return Baseline(mean=1.0, std=1.0, lower=0.0, upper=2.0, sample_count=len(vals))

    async def fake_save_and_merge(tenant_id, service, fresh_results):
        return []

    monkeypatch.setattr(analyzer.baseline_store, "compute_and_persist", fake_compute_and_persist)
    monkeypatch.setattr(analyzer.granger_store, "save_and_merge", fake_save_and_merge)

    req = AnalyzeRequest(tenant_id="tenant-cap", start=1, end=3600, step="15s", services=["payment-service"])
    report = await analyzer.run(DummyProvider(), req)
    assert len(report.metric_anomalies) <= 2
    assert len(report.root_causes) <= 1
    assert len(report.ranked_causes) <= 1
    assert len(report.anomaly_clusters) <= 1
    assert len(report.granger_results) <= 1
    assert any("capped" in warning for warning in report.analysis_warnings)


@pytest.mark.asyncio
async def test_analyzer_limits_uncorroborated_root_causes(monkeypatch):
    monkeypatch.setattr(analyzer, "DEFAULT_METRIC_QUERIES", ["q_a"])
    monkeypatch.setattr(analyzer, "get_registry", lambda: DummyRegistry())
    monkeypatch.setattr(analyzer.anomaly, "detect", fake_detect)
    monkeypatch.setattr(analyzer, "changepoint_detect", lambda ts, vals, threshold_sigma=None: [])
    monkeypatch.setattr(analyzer, "test_all_pairs", lambda series_map, max_lag=None, p_threshold=None: [])

    async def fake_compute_and_persist(tenant_id, metric_name, ts, vals, z_threshold=3.0):
        return Baseline(mean=1.0, std=1.0, lower=0.0, upper=2.0, sample_count=len(vals))

    async def fake_save_and_merge(tenant_id, service, fresh_results):
        return []

    def fake_generate(*args, **kwargs):
        return [
            RootCause(
                hypothesis="h1",
                confidence=0.2,
                severity=Severity.low,
                category=RcaCategory.unknown,
                evidence=[],
                contributing_signals=["metrics"],
                affected_services=[],
                recommended_action="investigate",
            ),
            RootCause(
                hypothesis="h2",
                confidence=0.21,
                severity=Severity.low,
                category=RcaCategory.unknown,
                evidence=[],
                contributing_signals=["metrics"],
                affected_services=[],
                recommended_action="investigate",
            ),
        ]

    def fake_rank(causes, correlated_events):
        return [
            RankedCause(root_cause=cause, ml_score=cause.confidence, final_score=cause.confidence, feature_importance={})
            for cause in causes
        ]

    monkeypatch.setattr(analyzer.baseline_store, "compute_and_persist", fake_compute_and_persist)
    monkeypatch.setattr(analyzer.granger_store, "save_and_merge", fake_save_and_merge)
    monkeypatch.setattr(analyzer.rca, "generate", fake_generate)
    monkeypatch.setattr(analyzer, "rank", fake_rank)

    req = AnalyzeRequest(tenant_id="tenant-quality", start=1, end=3600, step="15s", services=["payment-service"])
    report = await analyzer.run(DummyProvider(), req)

    assert len(report.root_causes) <= 1
    assert report.quality is not None
    assert report.quality.suppression_counts.get("root_causes_without_multisignal", 0) >= 1


class LogsFallbackProvider(EmptyProvider):
    def __init__(self):
        self.queries = []

    async def query_logs(self, query: str, start: int, end: int, limit=None):
        self.queries.append(query)
        if query == '{service=~"payment\\-service"}':
            return {
                "data": {
                    "result": [
                        {
                            "stream": {"job": "api"},
                            "values": [
                                [str(int(100 * 1e9)), "error one"],
                                [str(int(101 * 1e9)), "error two"],
                            ],
                        }
                    ]
                }
            }
        return {"data": {"result": []}}


@pytest.mark.asyncio
async def test_analyzer_retries_logs_with_global_selector_when_service_filter_returns_empty(monkeypatch):
    monkeypatch.setattr(analyzer, "DEFAULT_METRIC_QUERIES", ["q_a"])
    monkeypatch.setattr(analyzer, "get_registry", lambda: DummyRegistry())

    async def fake_process_metrics(provider, req, all_metric_queries, z_threshold, analysis_window_seconds):
        return [], [], [], [], {}, []

    monkeypatch.setattr(analyzer, "_process_metrics", fake_process_metrics)

    provider = LogsFallbackProvider()
    req = AnalyzeRequest(tenant_id="tenant-logs", start=1, end=3600, step="15s", services=["payment-service"])
    report = await analyzer.run(provider, req)

    assert '{service_name=~"payment\\-service"}' in provider.queries
    assert '{service=~"payment\\-service"}' in provider.queries
    assert not any("Logs query returned no entries" in warning for warning in report.analysis_warnings)


@pytest.mark.asyncio
async def test_analyzer_caps_predictive_only_critical_to_medium(monkeypatch):
    monkeypatch.setattr(analyzer, "DEFAULT_METRIC_QUERIES", ["q_a"])
    monkeypatch.setattr(analyzer, "get_registry", lambda: DummyRegistry())

    async def fake_process_metrics(provider, req, all_metric_queries, z_threshold, analysis_window_seconds):
        return (
            [],
            [],
            [SimpleNamespace(severity=Severity.critical)],
            [SimpleNamespace(severity=Severity.critical)],
            {},
            [],
        )

    monkeypatch.setattr(analyzer, "_process_metrics", fake_process_metrics)

    req = AnalyzeRequest(tenant_id="tenant-predictive", start=1, end=300, step="15s", services=["payment-service"])
    report = await analyzer.run(EmptyProvider(), req)

    assert report.overall_severity == Severity.medium
    assert any("severity was capped at MEDIUM" in warning for warning in report.analysis_warnings)
