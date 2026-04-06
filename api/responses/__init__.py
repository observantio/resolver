"""
Response models for API endpoints and internal data structures.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
"""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .analysis import AnalysisQuality, AnalysisReport, MetricSeriesDistributionStats
    from .anomalies import MetricAnomaly
    from .base import NpModel
    from .jobs import (
        AnalyzeConfigTemplateResponse,
        AnalyzeJobCreateResponse,
        AnalyzeJobListResponse,
        AnalyzeJobResultResponse,
        AnalyzeJobSummary,
        AnalyzeReportDeleteResponse,
        AnalyzeReportResponse,
        JobStatus,
    )
    from .logs import LogBurst, LogPattern
    from .rca import RootCause
    from .slo import BudgetStatus, SloBurnAlert
    from .traces import ErrorPropagation, ServiceLatency

_EXPORT_MODULES = {
    "NpModel": ".base",
    "MetricAnomaly": ".anomalies",
    "LogBurst": ".logs",
    "LogPattern": ".logs",
    "ServiceLatency": ".traces",
    "ErrorPropagation": ".traces",
    "RootCause": ".rca",
    "SloBurnAlert": ".slo",
    "BudgetStatus": ".slo",
    "AnalysisQuality": ".analysis",
    "AnalysisReport": ".analysis",
    "MetricSeriesDistributionStats": ".analysis",
    "JobStatus": ".jobs",
    "AnalyzeConfigTemplateResponse": ".jobs",
    "AnalyzeJobCreateResponse": ".jobs",
    "AnalyzeJobSummary": ".jobs",
    "AnalyzeJobListResponse": ".jobs",
    "AnalyzeJobResultResponse": ".jobs",
    "AnalyzeReportResponse": ".jobs",
    "AnalyzeReportDeleteResponse": ".jobs",
}

__all__ = [
    "AnalysisQuality",
    "AnalysisReport",
    "AnalyzeConfigTemplateResponse",
    "AnalyzeJobCreateResponse",
    "AnalyzeJobListResponse",
    "AnalyzeJobResultResponse",
    "AnalyzeJobSummary",
    "AnalyzeReportDeleteResponse",
    "AnalyzeReportResponse",
    "BudgetStatus",
    "ErrorPropagation",
    "JobStatus",
    "LogBurst",
    "LogPattern",
    "MetricAnomaly",
    "MetricSeriesDistributionStats",
    "NpModel",
    "RootCause",
    "ServiceLatency",
    "SloBurnAlert",
]


def __getattr__(name: str) -> Any:
    module_name = _EXPORT_MODULES.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module = import_module(module_name, __name__)
    value = getattr(module, name)
    globals()[name] = value
    return value
