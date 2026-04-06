"""
Ranking Logic for Root Cause Analysis, combining rule-based confidence with machine learning predictions based on
features extracted from root cause hypotheses and correlated events, to produce a final ranked list of potential causes
for observed anomalies.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
"""

from __future__ import annotations

from dataclasses import dataclass
from importlib import import_module
from typing import Protocol

import numpy as np

from config import settings
from engine.correlation.temporal import CorrelatedEvent
from engine.rca.hypothesis import RootCause


@dataclass(frozen=True)
class RankedCause:
    root_cause: RootCause
    ml_score: float
    final_score: float
    feature_importance: dict[str, float]


def _extract_features(cause: RootCause, event: CorrelatedEvent | None = None) -> list[float]:
    return [
        cause.confidence,
        cause.severity.weight() / settings.ranking_severity_divisor,
        len(cause.contributing_signals) / settings.ranking_signal_divisor,
        len(cause.affected_services) / settings.ranking_signal_divisor,
        1.0 if cause.deployment is not None else 0.0,
        len(event.metric_anomalies) / settings.ranking_event_count_divisor if event else 0.0,
        len(event.log_bursts) / settings.ranking_event_count_divisor if event else 0.0,
        len(event.service_latency) / settings.ranking_event_count_divisor if event else 0.0,
        event.confidence if event else 0.0,
    ]


_FEATURE_NAMES = [
    "rule_confidence",
    "severity_weight",
    "signal_count",
    "blast_radius",
    "has_deployment",
    "metric_anomaly_count",
    "log_burst_count",
    "latency_count",
    "correlation_confidence",
]


def _ranking_pseudo_labels(causes: list[RootCause]) -> list[int]:
    """
    Top half of hypotheses by rule confidence = positive class (avoids trivial single-class RF).
    """
    n = len(causes)
    order = sorted(range(n), key=lambda i: causes[i].confidence, reverse=True)
    labels = [0] * n
    half = max(1, n // 2)
    for i in range(half):
        labels[order[i]] = 1
    return labels


def _per_row_importance_share(row: np.ndarray, global_imp: np.ndarray) -> dict[str, float]:
    w = np.abs(row * global_imp)
    s = float(np.sum(w)) + 1e-12
    return dict(zip(_FEATURE_NAMES, (w / s).tolist()))


def _per_row_feature_shares(row: np.ndarray) -> dict[str, float]:
    w = np.abs(row)
    s = float(np.sum(w)) + 1e-12
    return dict(zip(_FEATURE_NAMES, (w / s).tolist()))


class RandomForestClassifierModel(Protocol):
    feature_importances_: np.ndarray

    def fit(self, data: np.ndarray, labels: list[int]) -> object: ...
    def predict_proba(self, data: np.ndarray) -> np.ndarray: ...


class RandomForestClassifierFactory(Protocol):
    def __call__(
        self,
        *,
        n_estimators: int,
        max_depth: int | None,
        random_state: int,
    ) -> RandomForestClassifierModel: ...


def rank(
    causes: list[RootCause],
    correlated_events: list[CorrelatedEvent] | None = None,
) -> list[RankedCause]:
    if not causes:
        return []

    events_map: dict[str, CorrelatedEvent] = {}
    if correlated_events:
        for ev in correlated_events:
            for a in ev.metric_anomalies:
                events_map[a.metric_name] = ev

    feature_matrix = []
    event_refs: list[CorrelatedEvent | None] = []
    for cause in causes:
        ref_metric = next(
            (s.split(":")[1] for s in cause.contributing_signals if s.startswith("metric:")),
            None,
        )
        event_ref: CorrelatedEvent | None = events_map.get(ref_metric) if ref_metric else None
        event_refs.append(event_ref)
        feature_matrix.append(_extract_features(cause, event_ref))

    x = np.array(feature_matrix, dtype=float)

    importances_global: np.ndarray | None = None
    try:
        random_forest_classifier: RandomForestClassifierFactory = import_module(
            "sklearn.ensemble"
        ).RandomForestClassifier

        if len(causes) >= 4:
            labels = _ranking_pseudo_labels(causes)
            if len(set(labels)) > 1:
                rf = random_forest_classifier(
                    n_estimators=settings.ranking_rf_n_estimators,
                    max_depth=settings.ranking_rf_max_depth,
                    random_state=settings.ranking_rf_random_state,
                )
                rf.fit(x, labels)
                ml_scores = rf.predict_proba(x)[:, 1]
                importances_global = rf.feature_importances_
            else:
                ml_scores = np.array([c.confidence for c in causes])
        else:
            ml_scores = np.array([c.confidence for c in causes])
    except ImportError:
        ml_scores = np.array([c.confidence for c in causes])
        importances_global = None

    results: list[RankedCause] = []
    for i, cause in enumerate(causes):
        ms = float(ml_scores[i])
        if importances_global is not None:
            row_imp = _per_row_importance_share(x[i], importances_global)
        else:
            row_imp = _per_row_feature_shares(x[i])
        final = round(
            settings.ranking_confidence_blend * cause.confidence + settings.ranking_ml_blend * ms,
            3,
        )
        results.append(
            RankedCause(
                root_cause=cause,
                ml_score=round(ms, 3),
                final_score=final,
                feature_importance=row_imp,
            )
        )

    return sorted(results, key=lambda r: r.final_score, reverse=True)
