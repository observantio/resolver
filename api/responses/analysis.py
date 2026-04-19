"""
Analysis report response models.

Copyright (c) 2026 Stefan Kumarasinghe.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. See http://www.apache.org/licenses/LICENSE-2.0 for details.
"""

from __future__ import annotations

from pydantic import Field

from engine.causal.bayesian import BayesianScore
from engine.causal.granger import GrangerResult
from engine.changepoint.cusum import ChangePoint
from engine.correlation.signals import LogMetricLink
from engine.enums import Severity
from engine.forecast.degradation import DegradationSignal
from engine.forecast.trajectory import TrajectoryForecast
from engine.ml.clustering import AnomalyCluster
from engine.ml.ranking import RankedCause

from .anomalies import MetricAnomaly
from .base import NpModel
from .logs import LogBurst, LogPattern
from .rca import RootCause
from .slo import SloBurnAlert
from .traces import ErrorPropagation, ServiceLatency


class MetricSeriesDistributionStats(NpModel):
    series_key: str = ""
    metric_name: str
    sample_count: int
    mean: float
    std: float
    min: float
    max: float
    median: float
    q1: float
    q3: float
    iqr: float
    mad: float
    skewness: float
    kurtosis: float
    coefficient_of_variation: float


class AnalysisQuality(NpModel):
    anomaly_density: dict[str, float] = Field(default_factory=dict)
    suppression_counts: dict[str, int] = Field(default_factory=dict)
    gating_profile: str
    confidence_calibration_version: str


class AnalysisReport(NpModel):
    tenant_id: str
    start: int
    end: int
    duration_seconds: int
    metric_anomalies: list[MetricAnomaly]
    log_bursts: list[LogBurst]
    log_patterns: list[LogPattern]
    service_latency: list[ServiceLatency]
    error_propagation: list[ErrorPropagation]
    slo_alerts: list[SloBurnAlert] = []
    root_causes: list[RootCause]
    ranked_causes: list[RankedCause] = []
    change_points: list[ChangePoint] = []
    log_metric_links: list[LogMetricLink] = []
    forecasts: list[TrajectoryForecast] = []
    degradation_signals: list[DegradationSignal] = []
    anomaly_clusters: list[AnomalyCluster] = []
    granger_results: list[GrangerResult] = []
    bayesian_scores: list[BayesianScore] = []
    analysis_warnings: list[str] = []
    overall_severity: Severity
    summary: str
    quality: AnalysisQuality | None = None
    metric_series_statistics: list[MetricSeriesDistributionStats] = Field(default_factory=list)
