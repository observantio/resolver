"""
Ranking Logic for Root Cause Analysis, combining rule-based confidence with machine learning predictions based on features extracted from root cause hypotheses and correlated events, to produce a final ranked list of potential causes for observed anomalies.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

import numpy as np

from config import settings
from engine.rca.hypothesis import RootCause
from engine.correlation.temporal import CorrelatedEvent


@dataclass(frozen=True)
class RankedCause:
    root_cause: RootCause
    ml_score: float
    final_score: float
    feature_importance: dict[str, float]



def _extract_features(cause: RootCause, event: Optional[CorrelatedEvent] = None) -> List[float]:
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
    "rule_confidence", "severity_weight", "signal_count",
    "blast_radius", "has_deployment", "metric_anomaly_count",
    "log_burst_count", "latency_count", "correlation_confidence",
]


def rank(
    causes: List[RootCause],
    correlated_events: Optional[List[CorrelatedEvent]] = None,
) -> List[RankedCause]:
    if not causes:
        return []

    events_map: dict[str, CorrelatedEvent] = {}
    if correlated_events:
        for ev in correlated_events:
            for a in ev.metric_anomalies:
                events_map[a.metric_name] = ev

    feature_matrix = []
    event_refs: List[Optional[CorrelatedEvent]] = []
    for cause in causes:
        ref_metric = next(
            (s.split(":")[1] for s in cause.contributing_signals if s.startswith("metric:")),
            None,
        )
        event_ref: Optional[CorrelatedEvent] = events_map.get(ref_metric) if ref_metric else None
        event_refs.append(event_ref)
        feature_matrix.append(_extract_features(cause, event_ref))

    X = np.array(feature_matrix, dtype=float)

    try:
        from sklearn.ensemble import RandomForestClassifier

        if len(causes) >= 4:
            labels = [1 if c.confidence >= settings.ranking_label_threshold else 0 for c in causes]
            if len(set(labels)) > 1:
                rf = RandomForestClassifier(
                    n_estimators=settings.ranking_rf_n_estimators,
                    max_depth=settings.ranking_rf_max_depth,
                    random_state=settings.ranking_rf_random_state,
                )
                rf.fit(X, labels)
                ml_scores = rf.predict_proba(X)[:, 1]
                importances = dict(zip(_FEATURE_NAMES, rf.feature_importances_))
            else:
                ml_scores = np.array([c.confidence for c in causes])
                importances = {n: 1.0 / len(_FEATURE_NAMES) for n in _FEATURE_NAMES}
        else:
            ml_scores = np.array([c.confidence for c in causes])
            importances = {n: 1.0 / len(_FEATURE_NAMES) for n in _FEATURE_NAMES}
    except ImportError:
        ml_scores = np.array([c.confidence for c in causes])
        importances = {n: 1.0 / len(_FEATURE_NAMES) for n in _FEATURE_NAMES}

    results: List[RankedCause] = []
    for cause, ml_score in zip(causes, ml_scores):
        final = round(
            settings.ranking_confidence_blend * cause.confidence
            + settings.ranking_ml_blend * float(ml_score),
            3,
        )
        results.append(RankedCause(
            root_cause=cause,
            ml_score=round(float(ml_score), 3),
            final_score=final,
            feature_importance=importances,
        ))

    return sorted(results, key=lambda r: r.final_score, reverse=True)
