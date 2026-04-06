"""
Test RCA hypothesis generation logic in the analysis engine, including creation of hypotheses based on correlated
signals, relevance scoring, and edge cases in timestamp handling and signal relationships.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
"""

from api.responses import MetricAnomaly, ServiceLatency
from engine.correlation.temporal import CorrelatedEvent
from engine.enums import ChangeType, RcaCategory, Severity
from engine.rca.hypothesis import RootCause, _action_for_category, _signals_from_event, generate


class DummyEvent:
    def __init__(self):
        self.metric_anomalies = []
        self.log_bursts = []
        self.service_latency = []
        self.window_start = 0
        self.confidence = 1.0


def test_signals_and_actions():
    ev = DummyEvent()
    ev.metric_anomalies = [
        MetricAnomaly(
            metric_id="m",
            metric_name="m",
            timestamp=1,
            value=0,
            change_type=ChangeType.SPIKE,
            z_score=5,
            mad_score=2,
            isolation_score=0.0,
            expected_range=(0, 1),
            severity=Severity.HIGH,
            description="",
        )
    ]
    ev.log_bursts = []
    ev.service_latency = []
    signals = _signals_from_event(ev)
    assert "metrics" in signals
    assert "deployment" in _action_for_category(RcaCategory.DEPLOYMENT)
    assert "resource" in _action_for_category(RcaCategory.RESOURCE_EXHAUSTION)
    assert "Investigate" in _action_for_category(None)


def test_generate_empty():
    root = generate([], [], [], [], [], correlated_events=[], graph=None, event_registry=None)
    assert root == []


def test_generate_with_simple_event():
    anomaly = MetricAnomaly(
        metric_id="m",
        metric_name="m",
        timestamp=1,
        value=100,
        change_type=ChangeType.SPIKE,
        z_score=10,
        mad_score=5,
        isolation_score=0.0,
        expected_range=(0, 1),
        severity=Severity.HIGH,
        description="",
    )
    ev = CorrelatedEvent(
        window_start=1,
        window_end=2,
        metric_anomalies=[anomaly],
        log_bursts=[],
        service_latency=[
            ServiceLatency(
                service="svc",
                operation="op",
                p50_ms=50.0,
                p95_ms=80.0,
                p99_ms=100.0,
                apdex=0.9,
                error_rate=0.0,
                sample_count=1,
                severity=Severity.LOW,
            )
        ],
        confidence=0.5,
    )
    root = generate([], [], [], [], [], correlated_events=[ev], graph=None, event_registry=None)
    assert isinstance(root, list)
    if root:
        assert isinstance(root[0], RootCause)
        assert "corroborating signal" in root[0].corroboration_summary


def test_generate_deduplicates_same_hypothesis_events():
    anomaly = MetricAnomaly(
        metric_id="m",
        metric_name="system_memory_usage_bytes",
        timestamp=1,
        value=100,
        change_type=ChangeType.SPIKE,
        z_score=10,
        mad_score=5,
        isolation_score=0.0,
        expected_range=(0, 1),
        severity=Severity.HIGH,
        description="",
    )
    ev1 = CorrelatedEvent(
        window_start=1,
        window_end=2,
        metric_anomalies=[anomaly],
        log_bursts=[],
        service_latency=[],
        confidence=0.6,
    )
    ev2 = CorrelatedEvent(
        window_start=1.5,
        window_end=2.5,
        metric_anomalies=[anomaly],
        log_bursts=[],
        service_latency=[],
        confidence=0.7,
    )
    causes = generate([], [], [], [], [], correlated_events=[ev1, ev2], graph=None, event_registry=None)
    assert len(causes) == 1
    assert causes[0].corroboration_summary


def test_hypothesis_prefers_high_impact_metrics_over_alphabetical():
    """
    Cited metrics should follow severity/z-score, not string sort on series names.
    """
    low_first_alpha = MetricAnomaly(
        metric_id="a",
        metric_name="process_cpu_time_seconds_total{process_pid=160}",
        timestamp=1,
        value=1,
        change_type=ChangeType.SPIKE,
        z_score=1.0,
        mad_score=1.0,
        isolation_score=0.0,
        expected_range=(0, 1),
        severity=Severity.LOW,
        description="",
    )
    critical_later_alpha = MetricAnomaly(
        metric_id="b",
        metric_name="process_cpu_time_seconds_total{process_pid=520145}",
        timestamp=1,
        value=9,
        change_type=ChangeType.SPIKE,
        z_score=10.0,
        mad_score=50.0,
        isolation_score=0.0,
        expected_range=(0, 1),
        severity=Severity.CRITICAL,
        description="",
    )
    ev = CorrelatedEvent(
        window_start=1,
        window_end=2,
        metric_anomalies=[low_first_alpha, critical_later_alpha],
        log_bursts=[],
        service_latency=[],
        confidence=0.8,
    )
    causes = generate([], [], [], [], [], correlated_events=[ev], graph=None, event_registry=None)
    assert causes
    hyp = causes[0].hypothesis
    assert "520145" in hyp
    assert hyp.index("520145") < hyp.index("160")


def test_generate_includes_process_entity_from_metric_labels():
    anomaly = MetricAnomaly(
        metric_id="m",
        metric_name=(
            "process_cpu_time_seconds_total{service_name=cache,process_executable_name=redis-server,process_pid=274}"
        ),
        timestamp=1,
        value=100,
        change_type=ChangeType.SPIKE,
        z_score=10,
        mad_score=5,
        isolation_score=0.0,
        expected_range=(0, 1),
        severity=Severity.HIGH,
        description="",
    )
    ev = CorrelatedEvent(
        window_start=1,
        window_end=2,
        metric_anomalies=[anomaly],
        log_bursts=[],
        service_latency=[],
        confidence=0.7,
    )
    causes = generate([], [], [], [], [], correlated_events=[ev], graph=None, event_registry=None)
    assert causes
    assert "process hotspot in redis-server(pid=274)" in causes[0].hypothesis
    assert any(str(item).startswith("process_entities=") for item in causes[0].evidence)
