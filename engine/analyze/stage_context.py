"""
Dataclasses for analyzer stage inputs.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
"""

from __future__ import annotations

import dataclasses

from api.responses import ErrorPropagation, LogBurst, LogPattern, MetricAnomaly, ServiceLatency
from engine.changepoint import ChangePoint
from engine.correlation import CorrelatedEvent
from engine.forecast.degradation import DegradationSignal
from engine.forecast.trajectory import TrajectoryForecast
from engine.ml import AnomalyCluster
from engine.topology import DependencyGraph


@dataclasses.dataclass(frozen=True)
class CorrelateStageInputs:
    metric_anomalies: list[MetricAnomaly]
    log_bursts: list[LogBurst]
    rca_log_bursts: list[LogBurst]
    service_latency: list[ServiceLatency]


@dataclasses.dataclass(frozen=True)
class CausalStageInputs:
    series_map: dict[str, list[float]]
    metric_anomalies: list[MetricAnomaly]
    rca_log_bursts: list[LogBurst]
    log_patterns: list[LogPattern]
    service_latency: list[ServiceLatency]
    error_propagation: list[ErrorPropagation]
    correlated_events: list[CorrelatedEvent]
    graph: DependencyGraph
    change_points: list[ChangePoint]
    forecasts: list[TrajectoryForecast]
    degradation_signals: list[DegradationSignal]
    anomaly_clusters: list[AnomalyCluster]


@dataclasses.dataclass(frozen=True)
class MetricsStageInputs:
    all_metric_queries: list[str]
    z_threshold: float
    analysis_window_seconds: float


@dataclasses.dataclass(frozen=True)
class TracesStageInputs:
    primary_service: str | None
    trace_filters: dict[str, str | int | float | bool]
    traces_raw: object
    warnings: list[str]


@dataclasses.dataclass(frozen=True)
class AnalysisScope:
    tenant_id: str
    primary_service: str | None
