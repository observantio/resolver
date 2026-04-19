"""
Analyze request models.

Copyright (c) 2026 Stefan Kumarasinghe.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. See http://www.apache.org/licenses/LICENSE-2.0 for details.
"""

from __future__ import annotations

from pydantic import Field

from ._time_range import TimeRangeRequest


class AnalyzeRequest(TimeRangeRequest):
    config_yaml: str | None = None
    sensitivity: float | None = Field(default=3.0, ge=1.0, le=6.0)
    apdex_threshold_ms: float = 500.0
    slo_target: float = Field(default=0.999, ge=0.0, le=1.0)
    correlation_window_seconds: float = Field(default=60.0, ge=10.0, le=600.0)
    forecast_horizon_seconds: float = Field(default=1800.0, ge=60.0, le=86400.0)


class AnalyzeJobCreateRequest(AnalyzeRequest):
    pass
