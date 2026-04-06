"""
Asynchronous analysis job response models.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum

from pydantic import BaseModel, Field

from custom_types.json import JSONDict


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
    started_at: datetime | None = None
    finished_at: datetime | None = None
    duration_ms: int | None = None
    error: str | None = None
    summary_preview: str | None = None
    tenant_id: str
    requested_by: str


class AnalyzeJobListResponse(BaseModel):
    items: list[AnalyzeJobSummary]
    next_cursor: str | None = None


class AnalyzeJobResultResponse(BaseModel):
    job_id: str
    report_id: str
    status: JobStatus
    tenant_id: str
    requested_by: str
    result: JSONDict | None = None


class AnalyzeReportResponse(BaseModel):
    job_id: str
    report_id: str
    status: JobStatus
    tenant_id: str
    requested_by: str
    result: JSONDict | None = None


class AnalyzeReportDeleteResponse(BaseModel):
    report_id: str
    status: JobStatus = Field(default=JobStatus.DELETED)
    deleted: bool = True


class AnalyzeConfigTemplateResponse(BaseModel):
    version: int
    defaults: JSONDict
    template_yaml: str
    file_name: str
