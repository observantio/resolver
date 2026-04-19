"""
Test cases for fuzzy testing of various components in the analysis engine, including anomaly detection, Granger
causality, forecasting, degradation analysis, correlation, causal graph logic, and topology graph logic. These tests use
randomized inputs to validate that the components can handle a wide range of scenarios without errors.

Copyright (c) 2026 Stefan Kumarasinghe.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. See http://www.apache.org/licenses/LICENSE-2.0 for details.
"""

import random

import pytest

from api.responses import LogBurst, MetricAnomaly, ServiceLatency
from engine.anomaly.detection import detect
from engine.causal.granger import GrangerAnalysisOptions, granger_multiple_pairs, granger_pair_analysis
from engine.causal.graph import CausalGraph
from engine.correlation.temporal import correlate
from engine.enums import Severity
from engine.forecast.degradation import analyze as degradation
from engine.forecast.trajectory import forecast
from engine.topology.graph import DependencyGraph


def random_anomaly(t):
    mid = f"m{random.randint(0, 5)}"
    return MetricAnomaly(
        metric_id=mid,
        metric_name=mid,
        timestamp=t,
        value=random.random() * 100,
        change_type="spike",
        z_score=random.random() * 5,
        mad_score=random.random() * 5,
        isolation_score=random.random(),
        expected_range=(0, 100),
        severity=random.choice(list(Severity)),
        description="",
    )


def random_logburst(t):
    return LogBurst(
        window_start=t,
        window_end=t + random.random() * 10,
        rate_per_second=random.random() * 5,
        baseline_rate=random.random() * 2 + 0.1,
        ratio=random.random() * 5,
        severity=random.choice(list(Severity)),
    )


def random_latency():
    return ServiceLatency(
        service=f"s{random.randint(0, 3)}",
        operation="op",
        p50_ms=random.random() * 100,
        p95_ms=random.random() * 200,
        p99_ms=random.random() * 300,
        apdex=random.random(),
        error_rate=random.random(),
        sample_count=random.randint(1, 10),
        severity=random.choice(list(Severity)),
    )


@pytest.mark.parametrize("seed", range(5))
def test_fuzzy_anomaly_detection(seed):
    random.seed(seed)
    length = random.randint(10, 50)
    ts = list(range(length))
    vals = [random.gauss(0, 1) for _ in range(length)]
    if length > 5:
        vals[random.randrange(length)] += random.choice([10, -10])
    anomalies = detect("m", ts, vals)
    assert isinstance(anomalies, list)
    for a in anomalies:
        assert hasattr(a, "change_type")


@pytest.mark.parametrize("seed", range(5))
def test_fuzzy_granger(seed):
    random.seed(seed)
    length = random.randint(15, 60)
    base = [random.random() for _ in range(length)]
    other = [b + random.gauss(0, 0.5) for b in base]
    res = granger_pair_analysis("a", base, "b", other, options=GrangerAnalysisOptions(max_lag=3))
    if res:
        assert res.cause_metric == "a"
    allr = granger_multiple_pairs({"a": base, "b": other})
    assert isinstance(allr, list)


@pytest.mark.parametrize("seed", range(5))
def test_fuzzy_forecast_and_degradation(seed):
    random.seed(seed)
    length = random.randint(10, 60)
    ts = list(range(length))
    vals = [random.random() * 100 + i * random.random() for i in ts]
    f = forecast("m", ts, vals, threshold=50, horizon_seconds=10)
    if f:
        assert isinstance(f, object)
    d = degradation("m", ts, vals)
    assert (d is None) or hasattr(d, "degradation_rate")


@pytest.mark.parametrize("seed", range(5))
def test_fuzzy_correlation_and_causal(seed):
    random.seed(seed)
    length = random.randint(10, 30)
    anomalies = [random_anomaly(i) for i in range(length)]
    bursts = [random_logburst(i * random.random() * 5) for i in range(length // 5)]
    sl = [random_latency() for _ in range(length // 5)]
    events = correlate(anomalies, bursts, sl, window_seconds=10)
    assert isinstance(events, list)

    g = CausalGraph()
    for i in range(5):
        a = f"m{i}"
        b = f"m{(i + 1) % 5}"
        g.add_edge(a, b, random.random())
    _ = g.topological_sort()
    _ = g.root_causes()
    _ = g.simulate_intervention("m0", max_depth=3)
    _ = g.find_common_causes("m1", "m2")


@pytest.mark.parametrize("seed", range(5))
def test_fuzzy_topology(seed):
    random.seed(seed)
    g = DependencyGraph()
    services = [f"s{i}" for i in range(5)]
    for _ in range(10):
        a = random.choice(services)
        b = random.choice(services)
        g.add_call(a, b)
    _ = g.blast_radius(random.choice(services), max_depth=3)
    _ = g.find_upstream_roots(random.choice(services))
    _ = g.critical_path(random.choice(services), random.choice(services))
    _ = g.all_services()
