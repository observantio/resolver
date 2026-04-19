"""
Clustering logic for grouping related anomalies based on temporal proximity and value similarity, using DBSCAN or a
simple fallback method when scikit-learn is unavailable.

Copyright (c) 2026 Stefan Kumarasinghe.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. See http://www.apache.org/licenses/LICENSE-2.0 for details.
"""

from __future__ import annotations

from dataclasses import dataclass
from importlib import import_module

import numpy as np

from api.responses import MetricAnomaly
from config import settings


@dataclass
class AnomalyCluster:
    cluster_id: int
    members: list[MetricAnomaly]
    centroid_timestamp: float
    centroid_value: float
    metric_names: list[str]
    size: int
    is_noise: bool = False


def _feature_matrix(anomalies: list[MetricAnomaly]) -> np.ndarray:
    ts_arr = np.array([a.timestamp for a in anomalies], dtype=float)
    val_arr = np.array([a.value for a in anomalies], dtype=float)
    ts_norm = (ts_arr - ts_arr.min()) / (np.ptp(ts_arr) + 1e-9)
    val_norm = (val_arr - val_arr.min()) / (np.ptp(val_arr) + 1e-9)
    return np.column_stack([ts_norm, val_norm])


def _cluster_one_metric(
    anomalies: list[MetricAnomaly],
    eps: float,
    min_samples: int,
) -> list[AnomalyCluster]:
    if len(anomalies) < min_samples:
        return []

    try:
        dbscan_factory = import_module("sklearn.cluster").DBSCAN
    except ImportError:
        return _fallback_cluster(anomalies)

    x = _feature_matrix(anomalies)
    model = dbscan_factory(eps=eps, min_samples=min_samples, metric="euclidean")
    labels = model.fit_predict(x)

    clusters: dict[int, list[MetricAnomaly]] = {}
    for label, anomaly in zip(labels, anomalies):
        clusters.setdefault(int(label), []).append(anomaly)

    result: list[AnomalyCluster] = []
    for cid, members in clusters.items():
        result.append(
            AnomalyCluster(
                cluster_id=cid,
                members=members,
                centroid_timestamp=float(np.mean([a.timestamp for a in members])),
                centroid_value=float(np.mean([a.value for a in members])),
                metric_names=list(dict.fromkeys(a.metric_name for a in members)),
                size=len(members),
                is_noise=cid == -1,
            )
        )

    return sorted(result, key=lambda c: c.size, reverse=True)


def cluster(
    anomalies: list[MetricAnomaly],
    eps: float | None = None,
    min_samples: int | None = None,
) -> list[AnomalyCluster]:
    if not anomalies:
        return []
    if eps is None:
        eps = settings.ml_cluster_eps
    if min_samples is None:
        min_samples = settings.ml_cluster_min_samples

    by_metric: dict[str, list[MetricAnomaly]] = {}
    for a in anomalies:
        by_metric.setdefault(a.metric_name or "", []).append(a)

    combined: list[AnomalyCluster] = []
    next_cluster_id = 0
    for _metric_key in sorted(by_metric.keys()):
        part = _cluster_one_metric(by_metric[_metric_key], eps, min_samples)
        for c in part:
            combined.append(
                AnomalyCluster(
                    cluster_id=next_cluster_id,
                    members=c.members,
                    centroid_timestamp=c.centroid_timestamp,
                    centroid_value=c.centroid_value,
                    metric_names=c.metric_names,
                    size=c.size,
                    is_noise=c.is_noise,
                )
            )
            next_cluster_id += 1

    return sorted(combined, key=lambda c: c.size, reverse=True)


def _fallback_cluster(anomalies: list[MetricAnomaly]) -> list[AnomalyCluster]:
    if not anomalies:
        return []
    return [
        AnomalyCluster(
            cluster_id=0,
            members=anomalies,
            centroid_timestamp=float(np.mean([a.timestamp for a in anomalies])),
            centroid_value=float(np.mean([a.value for a in anomalies])),
            metric_names=list(dict.fromkeys(a.metric_name for a in anomalies)),
            size=len(anomalies),
        )
    ]
