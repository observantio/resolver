"""
Core module implementing `jobs` functionality for the analysis engine.
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query, status
from api.requests import AnalyzeJobCreateRequest
from api.responses import (
    JobStatus,
    AnalyzeJobCreateResponse,
    AnalyzeJobListResponse,
    AnalyzeJobResultResponse,
    AnalyzeJobSummary,
    AnalyzeReportDeleteResponse,
    AnalyzeReportResponse,
)
from api.responses.jobs import AnalyzeJobSummary as JobView
from services.security_service import ensure_permission, get_internal_context
from services.security_service import InternalContext
from services.rca_job_service import rca_job_service

router = APIRouter(tags=["RCA Jobs"])


def _require_permission(name: str) -> None:
    ensure_permission(name)


def _required_context() -> InternalContext:
    ctx = get_internal_context()
    if ctx is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing internal context")
    return ctx


def _summary(job: JobView) -> AnalyzeJobSummary:
    return AnalyzeJobSummary(
        job_id=job.job_id,
        report_id=job.report_id,
        status=job.status,
        created_at=job.created_at,
        started_at=job.started_at,
        finished_at=job.finished_at,
        duration_ms=job.duration_ms,
        error=job.error,
        summary_preview=job.summary_preview,
        tenant_id=job.tenant_id,
        requested_by=job.requested_by,
    )


@router.post("/jobs/analyze", response_model=AnalyzeJobCreateResponse, status_code=status.HTTP_202_ACCEPTED)
async def create_job(payload: AnalyzeJobCreateRequest) -> AnalyzeJobCreateResponse:
    _require_permission("create:rca")
    ctx = _required_context()
    job = await rca_job_service.create_job(payload=payload, ctx=ctx)
    return AnalyzeJobCreateResponse(
        job_id=job.job_id,
        report_id=job.report_id,
        status=job.status,
        created_at=job.created_at,
        tenant_id=job.tenant_id,
        requested_by=job.requested_by,
    )


@router.get("/jobs", response_model=AnalyzeJobListResponse)
async def list_jobs(
    status_filter: JobStatus | None = Query(default=None, alias="status"),
    limit: int = Query(default=20, ge=1, le=100),
    cursor: str | None = Query(default=None),
) -> AnalyzeJobListResponse:
    _require_permission("read:rca")
    ctx = _required_context()
    items, next_cursor = await rca_job_service.list_jobs(
        ctx=ctx, status_filter=status_filter, limit=limit, cursor=cursor
    )
    return AnalyzeJobListResponse(items=[_summary(item) for item in items], next_cursor=next_cursor)


@router.get("/jobs/{job_id}", response_model=AnalyzeJobSummary)
async def get_job(job_id: str) -> AnalyzeJobSummary:
    _require_permission("read:rca")
    ctx = _required_context()
    job = await rca_job_service.get_job(job_id=job_id, ctx=ctx)
    return _summary(job)


@router.get("/jobs/{job_id}/result", response_model=AnalyzeJobResultResponse)
async def get_job_result(job_id: str) -> AnalyzeJobResultResponse:
    _require_permission("read:rca")
    ctx = _required_context()
    job, result = await rca_job_service.get_job_result(job_id=job_id, ctx=ctx)
    return AnalyzeJobResultResponse(
        job_id=job.job_id,
        report_id=job.report_id,
        status=job.status,
        tenant_id=job.tenant_id,
        requested_by=job.requested_by,
        result=result,
    )


@router.get("/reports/{report_id}", response_model=AnalyzeReportResponse)
async def get_report(report_id: str) -> AnalyzeReportResponse:
    _require_permission("read:rca")
    ctx = _required_context()
    job, result = await rca_job_service.get_report(report_id=report_id, ctx=ctx)
    return AnalyzeReportResponse(
        job_id=job.job_id,
        report_id=job.report_id,
        status=job.status,
        tenant_id=job.tenant_id,
        requested_by=job.requested_by,
        result=result,
    )


@router.delete("/reports/{report_id}", response_model=AnalyzeReportDeleteResponse)
async def delete_report(report_id: str) -> AnalyzeReportDeleteResponse:
    _require_permission("delete:rca")
    ctx = _required_context()
    await rca_job_service.delete_report(report_id=report_id, ctx=ctx)
    return AnalyzeReportDeleteResponse(report_id=report_id)
