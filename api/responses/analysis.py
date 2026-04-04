"""
Analysis report response models.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
"""

from __future__ import annotations

from typing import Dict, List, Optional

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

from .base import NpModel
from .anomalies import MetricAnomaly
from .logs import LogBurst, LogPattern
from .traces import ErrorPropagation, ServiceLatency
from .slo import SloBurnAlert
from .rca import RootCause


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
    anomaly_density: Dict[str, float] = Field(default_factory=dict)
    suppression_counts: Dict[str, int] = Field(default_factory=dict)
    gating_profile: str
    confidence_calibration_version: str


class AnalysisReport(NpModel):
    tenant_id: str
    start: int
    end: int
    duration_seconds: int
    metric_anomalies: List[MetricAnomaly]
    log_bursts: List[LogBurst]
    log_patterns: List[LogPattern]
    service_latency: List[ServiceLatency]
    error_propagation: List[ErrorPropagation]
    slo_alerts: List[SloBurnAlert] = []
    root_causes: List[RootCause]
    ranked_causes: List[RankedCause] = []
    change_points: List[ChangePoint] = []
    log_metric_links: List[LogMetricLink] = []
    forecasts: List[TrajectoryForecast] = []
    degradation_signals: List[DegradationSignal] = []
    anomaly_clusters: List[AnomalyCluster] = []
    granger_results: List[GrangerResult] = []
    bayesian_scores: List[BayesianScore] = []
    analysis_warnings: List[str] = []
    overall_severity: Severity
    summary: str
    quality: Optional[AnalysisQuality] = None
    metric_series_statistics: List[MetricSeriesDistributionStats] = Field(default_factory=list)
