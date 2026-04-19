"""
Scoring and categorization logic for RCA hypotheses based on deployment correlation, error propagation, and multi-
signal correlation patterns.

Copyright (c) 2026 Stefan Kumarasinghe.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. See http://www.apache.org/licenses/LICENSE-2.0 for details.
"""

from __future__ import annotations

import math

from api.responses import ErrorPropagation
from config import settings
from engine.correlation.temporal import CorrelatedEvent
from engine.enums import RcaCategory
from engine.events.registry import DeploymentEvent


def score_deployment_correlation(
    anomaly_ts: float,
    deployments: list[DeploymentEvent],
    window_seconds: float | None = None,
) -> float:
    if window_seconds is None:
        window_seconds = settings.rca_deploy_window_seconds
    nearby = [d for d in deployments if abs(d.timestamp - anomaly_ts) <= window_seconds]
    if not nearby:
        return 0.0
    closest_lag = min(abs(d.timestamp - anomaly_ts) for d in nearby)
    return round(max(0.0, 1.0 - closest_lag / window_seconds), 3)


def score_error_propagation(propagation: list[ErrorPropagation]) -> float:
    if not propagation:
        return 0.0
    affected = sum(len(getattr(p, "affected_services", [])) for p in propagation)
    base = settings.rca_baseline_base
    factor = settings.rca_baseline_affected_factor
    return round(min(settings.rca_errorprop_max, base + affected * factor), 3)


def score_correlated_event(event: CorrelatedEvent) -> float:
    configured = dict(settings.rca_weights or {})
    metric_weight = float(configured.get("metrics", configured.get("latency", 0.40)))
    log_weight = float(configured.get("logs", configured.get("log", 0.25)))
    trace_weight = float(configured.get("traces", configured.get("errors", 0.35)))
    metric_factor = min(1.0, math.log1p(len(event.metric_anomalies)) / math.log1p(200.0))
    log_factor = min(1.0, math.log1p(len(event.log_bursts)) / math.log1p(50.0))
    trace_factor = min(1.0, math.log1p(len(event.service_latency)) / math.log1p(50.0))

    metric_component = metric_weight * metric_factor
    log_component = log_weight * log_factor
    trace_component = trace_weight * trace_factor

    max_metric_severity = max(
        (a.severity.weight() for a in event.metric_anomalies),
        default=1,
    )
    severity_boost = 0.1 * min(1.0, float(max_metric_severity) / 8.0)

    blended = (metric_component + log_component + trace_component) * (0.7 + 0.3 * float(event.confidence))
    return round(min(settings.correlation_score_max, blended + severity_boost), 3)


def categorize(
    event: CorrelatedEvent,
    deployments: list[DeploymentEvent],
) -> RcaCategory:
    deploy_score = score_deployment_correlation(event.window_start, deployments) if deployments else 0.0

    if deploy_score > settings.rca_deploy_score_cutoff:
        return RcaCategory.DEPLOYMENT

    has_memory = any("memory" in a.metric_name or "mem" in a.metric_name for a in event.metric_anomalies)

    has_cpu = any("cpu" in a.metric_name for a in event.metric_anomalies)
    if has_memory or has_cpu:
        return RcaCategory.RESOURCE_EXHAUSTION

    if event.service_latency:
        return RcaCategory.DEPENDENCY_FAILURE

    has_traffic = any("request" in a.metric_name or "rate" in a.metric_name for a in event.metric_anomalies)
    if has_traffic:
        return RcaCategory.TRAFFIC_SURGE

    return RcaCategory.UNKNOWN
