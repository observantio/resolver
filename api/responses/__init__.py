"""
Response models for API endpoints and internal data structures.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime
from enum import Enum
import numpy as np
from pydantic import BaseModel, Field, model_serializer
from engine.enums import ChangeType, Severity, Signal


def _coerce(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {k: _coerce(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_coerce(v) for v in obj]
    if isinstance(obj, np.integer):
        return int(obj)
    if isinstance(obj, np.floating):
        return float(obj)
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    return obj


class NpModel(BaseModel):

    @model_serializer(mode="wrap")
    def _serialize(self, handler: Any) -> Any:
        return _coerce(handler(self))


class MetricAnomaly(NpModel):
    metric_name: str
    timestamp: float
    value: float
    change_type: ChangeType
    z_score: float
    mad_score: float
    isolation_score: float
    expected_range: Tuple[float, float]
    severity: Severity
    description: str


class LogBurst(NpModel):
    window_start: float
    window_end: float
    rate_per_second: float
    baseline_rate: float
    ratio: float
    severity: Severity


class LogPattern(NpModel):
    pattern: str
    count: int
    first_seen: float
    last_seen: float
    rate_per_minute: float
    entropy: float
    severity: Severity
    sample: str


class ServiceLatency(NpModel):
    service: str
    operation: str
    p50_ms: float
    p95_ms: float
    p99_ms: float
    apdex: float
    error_rate: float
    sample_count: int
    severity: Severity
    window_start: Optional[float] = None
    window_end: Optional[float] = None


class ErrorPropagation(NpModel):
    source_service: str
    affected_services: List[str]
    error_rate: float
    severity: Severity


class RootCause(NpModel):
    hypothesis: str
    confidence: float = Field(ge=0.0, le=1.0)
    evidence: List[str]
    contributing_signals: List[Signal]
    recommended_action: str
    severity: Severity
    corroboration_summary: Optional[str] = None
    suppression_diagnostics: Dict[str, Any] = Field(default_factory=dict)
    selection_score_components: Dict[str, float] = Field(default_factory=dict)


class SloBurnAlert(NpModel):
    service: str
    window_label: str
    error_rate: float
    burn_rate: float
    budget_consumed_pct: float
    severity: Severity


class BudgetStatus(NpModel):
    service: str
    target_availability: float
    current_availability: float
    budget_used_pct: float
    remaining_minutes: float
    on_track: bool


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

class JobStatus(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    DELETED = "deleted"


class AnalyzeJobCreateResponse(BaseModel):
    job_id: str
    report_id: str
    status: JobStatus
    created_at: datetime
    tenant_id: str
    requested_by: str


class AnalyzeJobSummary(BaseModel):
    job_id: str
    report_id: str
    status: JobStatus
    created_at: datetime
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    duration_ms: Optional[int] = None
    error: Optional[str] = None
    summary_preview: Optional[str] = None
    tenant_id: str
    requested_by: str


class AnalyzeJobListResponse(BaseModel):
    items: list[AnalyzeJobSummary]
    next_cursor: Optional[str] = None


class AnalyzeJobResultResponse(BaseModel):
    job_id: str
    report_id: str
    status: JobStatus
    tenant_id: str
    requested_by: str
    result: Optional[dict[str, Any]] = None


class AnalyzeReportResponse(BaseModel):
    job_id: str
    report_id: str
    status: JobStatus
    tenant_id: str
    requested_by: str
    result: Optional[dict[str, Any]] = None


class AnalyzeReportDeleteResponse(BaseModel):
    report_id: str
    status: JobStatus = Field(default=JobStatus.DELETED)
    deleted: bool = True
