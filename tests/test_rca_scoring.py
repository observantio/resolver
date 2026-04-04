"""
Test cases for scoring logic in the RCA component of the analysis engine, including relevance scoring of root cause
candidates based on signal strength, temporal proximity, and category-specific heuristics.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
"""

from api.responses import MetricAnomaly, ServiceLatency
from engine.correlation.temporal import CorrelatedEvent
from engine.enums import ChangeType, RcaCategory, Severity
from engine.events.registry import DeploymentEvent
from engine.rca.scoring import categorize, score_correlated_event


def _anomaly(metric_name: str, severity: Severity = Severity.HIGH) -> MetricAnomaly:
    return MetricAnomaly(
        metric_name=metric_name,
        timestamp=100.0,
        value=1.0,
        change_type=ChangeType.SPIKE,
        z_score=4.0,
        mad_score=4.0,
        isolation_score=-0.3,
        expected_range=(0.0, 1.0),
        severity=severity,
        description="a",
    )


def _event(
    metric_names: list[str], latency_services: list[str] | None = None, confidence: float = 0.8
) -> CorrelatedEvent:
    latency_services = latency_services or []
    return CorrelatedEvent(
        window_start=100.0,
        window_end=140.0,
        metric_anomalies=[_anomaly(name) for name in metric_names],
        log_bursts=[],
        service_latency=[
            ServiceLatency(
                service=svc,
                operation="op",
                p50_ms=10,
                p95_ms=20,
                p99_ms=30,
                apdex=0.8,
                error_rate=0.0,
                sample_count=10,
                severity=Severity.MEDIUM,
            )
            for svc in latency_services
        ],
        signal_count=max(1, len(metric_names) + len(latency_services)),
        confidence=confidence,
    )


def test_categorize_prefers_deployment_when_nearby():
    event = _event(["system_memory_usage_bytes"])
    deployments = [DeploymentEvent(service="checkout", timestamp=100.0, version="v1")]
    assert categorize(event, deployments) == RcaCategory.DEPLOYMENT


def test_categorize_resource_exhaustion_for_memory_cpu():
    assert categorize(_event(["system_memory_usage_bytes"]), []) == RcaCategory.RESOURCE_EXHAUSTION
    assert categorize(_event(["node_cpu_seconds_total"]), []) == RcaCategory.RESOURCE_EXHAUSTION


def test_categorize_dependency_failure_from_latency():
    assert categorize(_event(["custom_metric"], latency_services=["payments"]), []) == RcaCategory.DEPENDENCY_FAILURE


def test_categorize_traffic_from_request_rate_metrics():
    assert categorize(_event(["http_request_rate_total"]), []) == RcaCategory.TRAFFIC_SURGE


def test_score_correlated_event_monotonic_with_more_signals():
    sparse = _event(["metric_a"], latency_services=[], confidence=0.4)
    dense = _event(["metric_a", "metric_b", "metric_c"], latency_services=["svc1", "svc2"], confidence=0.9)
    assert score_correlated_event(dense) >= score_correlated_event(sparse)
