"""
Analysis report response models.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import Field

from engine.enums import Severity

from .base import NpModel
from .anomalies import MetricAnomaly
from .logs import LogBurst, LogPattern
from .traces import ErrorPropagation, ServiceLatency
from .slo import SloBurnAlert
from .rca import RootCause


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
    ranked_causes: List[Any] = []
    change_points: List[Any] = []
    log_metric_links: List[Any] = []
    forecasts: List[Any] = []
    degradation_signals: List[Any] = []
    anomaly_clusters: List[Any] = []
    granger_results: List[Any] = []
    bayesian_scores: List[Any] = []
    analysis_warnings: List[str] = []
    overall_severity: Severity
    summary: str
    quality: Optional[AnalysisQuality] = None
