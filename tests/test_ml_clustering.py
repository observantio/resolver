"""
Tests for metric-aware anomaly clustering.

Copyright (c) 2026 Stefan Kumarasinghe.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. See http://www.apache.org/licenses/LICENSE-2.0 for details.
"""

from __future__ import annotations

import numpy as np

import engine.ml.clustering as clustering
from api.responses import MetricAnomaly
from engine.enums import ChangeType, Severity


def _anomaly(
    metric_name: str,
    timestamp: float,
    value: float,
) -> MetricAnomaly:
    return MetricAnomaly(
        metric_name=metric_name,
        timestamp=timestamp,
        value=value,
        change_type=ChangeType.SPIKE,
        z_score=1.0,
        mad_score=1.0,
        isolation_score=-0.1,
        expected_range=(0.0, 1.0),
        severity=Severity.MEDIUM,
        description="",
    )


def test_cluster_empty_returns_empty():
    assert clustering.cluster([]) == []


def test_cluster_splits_by_metric_name():
    a = [
        _anomaly("m1", 1.0, 1.0),
        _anomaly("m1", 2.0, 2.0),
        _anomaly("m2", 100.0, 100.0),
        _anomaly("m2", 101.0, 101.0),
    ]
    out = clustering.cluster(a, eps=0.5, min_samples=2)
    assert len(out) >= 1
    for c in out:
        member_metrics = {m.metric_name for m in c.members}
        assert len(member_metrics) == 1


def test_cluster_sklearn_path_produces_clusters():
    pts = [_anomaly("single", float(i), float(i)) for i in range(5)]
    out = clustering.cluster(pts, eps=0.3, min_samples=2)
    assert isinstance(out, list)
    assert all(c.cluster_id >= 0 for c in out)


def test_cluster_import_error_uses_fallback(monkeypatch):
    def boom(_name):
        raise ImportError("no sklearn")

    monkeypatch.setattr(clustering, "import_module", boom)
    pts = [_anomaly("m", float(i), float(i)) for i in range(3)]
    out = clustering.cluster(pts, eps=0.5, min_samples=2)
    assert len(out) == 1
    assert out[0].size == 3


def test_fallback_cluster_empty():
    assert clustering._fallback_cluster([]) == []


def test_feature_matrix_single_point():
    a = [_anomaly("m", 1.0, 2.0)]
    x = clustering._feature_matrix(a)
    assert x.shape == (1, 2)
    assert np.allclose(x, np.array([[0.0, 0.0]]))
