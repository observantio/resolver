"""
Test cases for correlation logic in the analysis engine, validating temporal correlation of anomalies, log bursts, and
service latency, as well as edge cases in timestamp handling and relevance filtering.

Copyright (c) 2026 Stefan Kumarasinghe.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. See http://www.apache.org/licenses/LICENSE-2.0 for details.
"""

from api.responses import LogBurst, MetricAnomaly, ServiceLatency
from config import settings
from engine.correlation.signals import link_logs_to_metrics
from engine.correlation.temporal import CorrelatedEvent, correlate
from engine.enums import ChangeType, Severity


def make_anomaly(t):
    return MetricAnomaly(
        metric_name="m",
        timestamp=t,
        value=1,
        change_type=ChangeType.SPIKE,
        z_score=1,
        mad_score=1,
        isolation_score=0,
        expected_range=(0, 1),
        severity=Severity.LOW,
        description="",
    )


def make_logburst(start, end):
    return LogBurst(
        window_start=start, window_end=end, rate_per_second=1, baseline_rate=1, ratio=1, severity=Severity.LOW
    )


def make_latency(service="s", window_start=0, window_end=60):
    return ServiceLatency(
        service=service,
        operation="o",
        p50_ms=10,
        p95_ms=20,
        p99_ms=30,
        apdex=0.5,
        error_rate=0,
        sample_count=1,
        severity=Severity.LOW,
        window_start=window_start,
        window_end=window_end,
    )


def test_correlate_simple():
    anomalies = [make_anomaly(0), make_anomaly(100)]
    bursts = [make_logburst(0, 10)]
    sl = [make_latency()]
    events = correlate(anomalies, bursts, sl, window_seconds=200)
    assert isinstance(events, list)
    assert isinstance(events, list)
    assert events and isinstance(events[0], CorrelatedEvent)


def test_correlate_empty():
    assert correlate([], [], [], window_seconds=10) == []


def test_link_logs_to_metrics_uses_window_start_fields():
    links = link_logs_to_metrics([make_anomaly(10)], [make_logburst(9, 11)], max_lag_seconds=20)
    assert links
    assert links[0].log_stream == "unknown"


def test_correlate_ignores_invalid_logburst_timestamps():
    anomalies = [make_anomaly(100)]

    class BrokenBurst:
        start = "bad"
        end = "bad"

    bursts = [BrokenBurst()]
    events = correlate(anomalies, bursts, [], window_seconds=30)
    assert events == []


def test_correlate_only_links_temporally_and_name_relevant_latency():
    anomalies = [
        MetricAnomaly(
            metric_name="system_memory_usage_bytes{service=checkout}",
            timestamp=100,
            value=1,
            change_type=ChangeType.SPIKE,
            z_score=4,
            mad_score=4,
            isolation_score=0,
            expected_range=(0, 1),
            severity=Severity.HIGH,
            description="",
        )
    ]
    bursts = [make_logburst(95, 105)]
    latency = [
        ServiceLatency(
            service="checkout",
            operation="o",
            p50_ms=10,
            p95_ms=20,
            p99_ms=30,
            apdex=0.5,
            error_rate=0,
            sample_count=1,
            severity=Severity.LOW,
            window_start=90.0,
            window_end=110.0,
        ),
        ServiceLatency(
            service="inventory",
            operation="o",
            p50_ms=10,
            p95_ms=20,
            p99_ms=30,
            apdex=0.5,
            error_rate=0,
            sample_count=1,
            severity=Severity.LOW,
            window_start=90.0,
            window_end=110.0,
        ),
    ]
    events = correlate(anomalies, bursts, latency, window_seconds=30)
    assert events
    assert [s.service for s in events[0].service_latency] == ["checkout"]


def test_correlate_requires_temporal_overlap_for_latency():
    anomalies = [
        MetricAnomaly(
            metric_name="request_total{service=checkout}",
            timestamp=100,
            value=1,
            change_type=ChangeType.SPIKE,
            z_score=4,
            mad_score=4,
            isolation_score=0,
            expected_range=(0, 1),
            severity=Severity.HIGH,
            description="",
        )
    ]
    bursts = [make_logburst(95, 105)]
    latency = [make_latency(service="checkout", window_start=400, window_end=450)]
    events = correlate(anomalies, bursts, latency, window_seconds=30)
    assert events
    assert events[0].service_latency == []


def test_correlate_does_not_use_substring_service_match():
    anomalies = [
        MetricAnomaly(
            metric_name="request_total{service=cart}",
            timestamp=100,
            value=1,
            change_type=ChangeType.SPIKE,
            z_score=4,
            mad_score=4,
            isolation_score=0,
            expected_range=(0, 1),
            severity=Severity.HIGH,
            description="",
        )
    ]
    bursts = [make_logburst(95, 105)]
    latency = [make_latency(service="cart-db", window_start=95, window_end=105)]
    events = correlate(anomalies, bursts, latency, window_seconds=30)
    assert events
    assert events[0].service_latency == []


def test_correlate_with_custom_weight_fn():
    # make sure weight_fn override is applied and respects score cap
    anomalies = [make_anomaly(0)]
    bursts = [make_logburst(0, 1)]
    sl = [make_latency()]
    # compute unweighted metric score for reference
    m_score = min(settings.correlation_score_max, 1 * settings.correlation_weight_time)

    # define a weight function that doubles metric component only
    def wfn(m, log_score, t):
        return m * 2

    events = correlate(anomalies, bursts, sl, window_seconds=10, weight_fn=wfn)
    assert events
    expected = round(min(settings.correlation_score_max, wfn(m_score, 0, 0)), 3)
    assert events[0].confidence == expected
