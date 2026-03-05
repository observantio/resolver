"""
Asynchronous analysis job response models.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field


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
