"""
Requests and data models for API endpoints.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
"""

from __future__ import annotations

from typing import List, Optional
from pydantic import BaseModel, Field, model_validator


class AnalyzeRequest(BaseModel):
    tenant_id: str
    start: int
    end: int
    step: str = "15s"
    services: List[str] = Field(default_factory=list)
    log_query: Optional[str] = None
    metric_queries: Optional[List[str]] = None
    sensitivity: Optional[float] = Field(default=3.0, ge=1.0, le=6.0)
    apdex_threshold_ms: float = 500.0
    slo_target: float = Field(default=0.999, ge=0.0, le=1.0)
    correlation_window_seconds: float = Field(default=60.0, ge=10.0, le=600.0)
    forecast_horizon_seconds: float = Field(default=1800.0, ge=60.0, le=86400.0)

    @model_validator(mode="after")
    def validate_time_range(self) -> "AnalyzeRequest":
        if self.start >= self.end:
            raise ValueError("start must be less than end")
        return self


class MetricRequest(BaseModel):
    tenant_id: str
    query: str
    start: int
    end: int
    step: str = "15s"
    sensitivity: Optional[float] = Field(default=3.0, ge=1.0, le=6.0)


class LogRequest(BaseModel):
    tenant_id: str
    query: str
    start: int
    end: int


class TraceRequest(BaseModel):
    tenant_id: str
    start: int
    end: int
    service: Optional[str] = None
    apdex_threshold_ms: float = 500.0


class SloRequest(BaseModel):
    tenant_id: str
    service: str
    start: int
    end: int
    step: str = "15s"
    target_availability: float = Field(default=0.999, ge=0.0, le=1.0)
    error_query: Optional[str] = None
    total_query: Optional[str] = None

    @model_validator(mode="after")
    def validate_time_range(self) -> "SloRequest":
        if self.start >= self.end:
            raise ValueError("start must be less than end")
        return self


class CorrelateRequest(BaseModel):
    tenant_id: str
    start: int
    end: int
    step: str = "15s"
    services: List[str] = Field(default_factory=list)
    log_query: Optional[str] = None
    metric_queries: Optional[List[str]] = None
    window_seconds: float = Field(default=60.0, ge=10.0, le=600.0)

    @model_validator(mode="after")
    def validate_time_range(self) -> "CorrelateRequest":
        if self.start >= self.end:
            raise ValueError("start must be less than end")
        return self


class ChangepointRequest(BaseModel):
    tenant_id: str
    query: str
    start: int
    end: int
    step: str = "15s"
    threshold_sigma: float = Field(default=4.0, ge=1.0, le=10.0)


class TopologyRequest(BaseModel):
    tenant_id: str
    start: int
    end: int
    root_service: str
    max_depth: int = Field(default=6, ge=1, le=10)


class DeploymentEventRequest(BaseModel):
    tenant_id: str
    service: str
    timestamp: float
    version: str
    author: str = ""
    environment: str = "production"
    source: str = "api"
    metadata: dict = Field(default_factory=dict)


class AnalyzeJobCreateRequest(AnalyzeRequest):
    pass
