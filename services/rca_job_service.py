"""
RCA job management service that handles scheduling, execution, and lifecycle of root cause analysis jobs.

Copyright (c) 2026 Stefan Kumarasinghe
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
"""

from __future__ import annotations

import asyncio
import base64
import binascii
import hashlib
import json
import logging
import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import HTTPException, status
from sqlalchemy import and_, or_, select

from api.responses import JobStatus
from api.responses.jobs import AnalyzeJobSummary as JobView
from api.requests import AnalyzeRequest
from services.analyze_service import run_analysis
from services.analysis_config_service import analysis_config_service
from services.security_service import InternalContext
from config import settings
from custom_types.json import JSONDict
from database import get_db_session
from db_models import RcaJob, RcaReport


_JOB_EXECUTION_ERRORS = (
    asyncio.TimeoutError,
    OSError,
    RuntimeError,
    TypeError,
    ValueError,
)

logger = logging.getLogger(__name__)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _coerce_datetime(value: object) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value
    return _utcnow()


def _coerce_optional_datetime(value: object) -> Optional[datetime]:
    if value is None:
        return None
    return _coerce_datetime(value)


def _duration_ms(start: object, end: object) -> int:
    start_dt = _coerce_datetime(start)
    end_dt = _coerce_datetime(end)
    return max(0, int((end_dt - start_dt).total_seconds() * 1000))


def _to_view(row: RcaJob) -> JobView:
    return JobView(
        job_id=row.job_id,
        report_id=row.report_id,
        status=JobStatus(row.status),
        created_at=_coerce_datetime(row.created_at),
        tenant_id=row.tenant_id,
        requested_by=row.requested_by,
        started_at=_coerce_optional_datetime(row.started_at),
        finished_at=_coerce_optional_datetime(row.finished_at),
        duration_ms=row.duration_ms,
        error=row.error,
        summary_preview=row.summary_preview,
    )


def _encode_cursor(*, created_at: datetime, job_id: str) -> str:
    payload = {"created_at": created_at.isoformat(), "job_id": job_id}
    raw = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii")


def _decode_cursor(cursor: Optional[str]) -> tuple[Optional[datetime], Optional[str]]:
    if not cursor:
        return None, None
    try:
        raw = base64.urlsafe_b64decode(cursor.encode("ascii")).decode("utf-8")
        payload = json.loads(raw)
        created_at = datetime.fromisoformat(str(payload.get("created_at")))
        if created_at.tzinfo is None:
            created_at = created_at.replace(tzinfo=timezone.utc)
        job_id = str(payload.get("job_id") or "").strip()
        if not job_id:
            return None, None
        return created_at, job_id
    except (ValueError, TypeError, json.JSONDecodeError, binascii.Error):
        return None, None


class RcaJobService:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._semaphore = asyncio.Semaphore(max(1, int(settings.analyze_max_concurrency)))
        self._tasks: dict[str, asyncio.Task[None]] = {}

    @staticmethod
    def _fingerprint(payload: JSONDict) -> str:
        canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    async def startup_recovery(self) -> None:
        await asyncio.to_thread(self._startup_recovery_sync)
        await self.cleanup_retention()

    def _startup_recovery_sync(self) -> None:
        now = _utcnow()
        with get_db_session() as db:
            stale = db.scalars(
                select(RcaJob).where(RcaJob.status.in_([JobStatus.QUEUED.value, JobStatus.RUNNING.value]))
            ).all()
            for row in stale:
                row.status = JobStatus.FAILED.value
                row.finished_at = now
                row.error = "Interrupted due to process restart before completion"
                anchor = row.started_at or row.created_at
                row.duration_ms = _duration_ms(anchor, now)

    async def cleanup_retention(self) -> None:
        await asyncio.to_thread(self._cleanup_retention_sync)

    def _cleanup_retention_sync(self) -> None:
        now = _utcnow()
        report_retention_days = max(0, int(settings.analyze_report_retention_days))
        job_ttl_days = max(1, int(settings.analyze_job_ttl_days))
        report_cutoff = now - timedelta(days=report_retention_days)
        job_cutoff = now - timedelta(days=job_ttl_days)

        with get_db_session() as db:
            if report_retention_days > 0:
                expired_reports = db.scalars(
                    select(RcaReport).where(and_(RcaReport.expires_at.is_not(None), RcaReport.expires_at < now))
                ).all()
                for report in expired_reports:
                    report.result_payload = None

                old_reports = db.scalars(
                    select(RcaReport).where(and_(RcaReport.expires_at.is_(None), RcaReport.created_at < report_cutoff))
                ).all()
                for report in old_reports:
                    report.result_payload = None

            stale_jobs = db.scalars(
                select(RcaJob).where(and_(RcaJob.created_at < job_cutoff, RcaJob.status.in_([
                    JobStatus.COMPLETED.value, JobStatus.FAILED.value, JobStatus.CANCELLED.value, JobStatus.DELETED.value,
                ])))
            ).all()
            for job in stale_jobs:
                db.delete(job)

    async def create_job(self, *, payload: AnalyzeRequest, ctx: InternalContext) -> JobView:
        now = _utcnow()
        tenant_payload = payload.model_copy(update={"tenant_id": ctx.tenant_id})
        analysis_config_service.prepare_request(
            tenant_payload,
            explicit_fields=set(payload.model_fields_set),
        )
        materialized_payload = tenant_payload.model_dump(exclude_none=True)
        materialized_payload["explicit_request_fields"] = sorted(set(payload.model_fields_set))
        job_id = str(uuid.uuid4())
        report_id = str(uuid.uuid4())

        def _create() -> JobView:
            with get_db_session() as db:
                row = RcaJob(
                    job_id=job_id,
                    report_id=report_id,
                    tenant_id=ctx.tenant_id,
                    requested_by=ctx.user_id,
                    status=JobStatus.QUEUED.value,
                    created_at=now,
                    request_fingerprint=self._fingerprint(materialized_payload),
                    request_payload=materialized_payload,
                )
                db.add(row)
                return _to_view(row)

        created = await asyncio.to_thread(_create)
        task = asyncio.create_task(self._run_job(job_id=job_id))
        async with self._lock:
            self._tasks[job_id] = task
        return created

    async def _run_job(self, *, job_id: str) -> None:
        try:
            async with self._semaphore:
                row = await asyncio.to_thread(self._get_job_row, job_id)
                if row is None:
                    return

                started_at = _utcnow()
                await asyncio.to_thread(self._mark_running, job_id, started_at)
                try:
                    request_payload = dict(row.request_payload)
                    explicit_fields_raw = request_payload.pop("explicit_request_fields", [])
                    explicit_fields_iterable = explicit_fields_raw if isinstance(explicit_fields_raw, list) else []
                    explicit_fields = {str(item) for item in explicit_fields_iterable if str(item)}
                    req = AnalyzeRequest.model_validate(request_payload)
                    prepared = analysis_config_service.prepare_request(req, explicit_fields=explicit_fields)
                    result_model = await asyncio.wait_for(
                        run_analysis(req, explicit_fields=explicit_fields, prepared=prepared),
                        timeout=prepared.timeout_seconds,
                    )
                    result = result_model.model_dump() if hasattr(result_model, "model_dump") else dict(result_model)
                    finished_at = _utcnow()
                    await asyncio.to_thread(self._mark_completed, job_id, finished_at, result)
                except asyncio.CancelledError:
                    await asyncio.to_thread(self._mark_cancelled, job_id, _utcnow(), "Cancelled by report owner")
                    raise
                except _JOB_EXECUTION_ERRORS as exc:
                    await asyncio.to_thread(self._mark_failed, job_id, _utcnow(), str(exc))
                except Exception as exc:  # pylint: disable=broad-exception-caught
                    logger.exception("Unexpected RCA job execution error for job_id=%s", job_id)
                    await asyncio.to_thread(self._mark_failed, job_id, _utcnow(), str(exc))
        finally:
            async with self._lock:
                self._tasks.pop(job_id, None)

    def _get_job_row(self, job_id: str) -> Optional[RcaJob]:
        with get_db_session() as db:
            return db.get(RcaJob, job_id)

    def _mark_running(self, job_id: str, started_at: datetime) -> None:
        with get_db_session() as db:
            row = db.get(RcaJob, job_id)
            if row is None or row.status in {JobStatus.DELETED.value, JobStatus.CANCELLED.value}:
                return
            row.status = JobStatus.RUNNING.value
            row.started_at = started_at
            row.error = None

    def _mark_completed(self, job_id: str, finished_at: datetime, result: JSONDict) -> None:
        with get_db_session() as db:
            row = db.get(RcaJob, job_id)
            if row is None or row.status in {JobStatus.DELETED.value, JobStatus.CANCELLED.value}:
                return
            row.status = JobStatus.COMPLETED.value
            row.finished_at = finished_at
            anchor = row.started_at or row.created_at
            row.duration_ms = _duration_ms(anchor, finished_at)
            row.summary_preview = str(result.get("summary") or "")[:280] or None
            row.error = None
            existing = db.get(RcaReport, row.report_id)
            expires_at = None
            if int(settings.analyze_report_retention_days) > 0:
                expires_at = finished_at + timedelta(days=int(settings.analyze_report_retention_days))
            if existing is None:
                db.add(RcaReport(
                    report_id=row.report_id,
                    job_id=row.job_id,
                    tenant_id=row.tenant_id,
                    owner_user_id=row.requested_by,
                    result_payload=result,
                    created_at=finished_at,
                    expires_at=expires_at,
                ))
            else:
                existing.result_payload = result
                existing.expires_at = expires_at

    def _mark_failed(self, job_id: str, finished_at: datetime, error: str) -> None:
        with get_db_session() as db:
            row = db.get(RcaJob, job_id)
            if row is None or row.status in {JobStatus.DELETED.value, JobStatus.CANCELLED.value}:
                return
            row.status = JobStatus.FAILED.value
            row.finished_at = finished_at
            anchor = row.started_at or row.created_at
            row.duration_ms = _duration_ms(anchor, finished_at)
            row.error = (error or "Analysis failed")[:500]

    def _mark_cancelled(self, job_id: str, finished_at: datetime, error: str) -> None:
        with get_db_session() as db:
            row = db.get(RcaJob, job_id)
            if row is None or row.status == JobStatus.DELETED.value:
                return
            row.status = JobStatus.CANCELLED.value
            row.finished_at = finished_at
            anchor = row.started_at or row.created_at
            row.duration_ms = _duration_ms(anchor, finished_at)
            row.error = error[:500]

    async def list_jobs(
        self,
        *,
        ctx: InternalContext,
        status_filter: Optional[JobStatus],
        limit: int,
        cursor: Optional[str],
    ) -> tuple[list[JobView], Optional[str]]:
        def _list() -> tuple[list[JobView], Optional[str]]:
            with get_db_session() as db:
                page_size = max(1, min(100, int(limit)))
                stmt = select(RcaJob).where(
                    and_(
                        RcaJob.tenant_id == ctx.tenant_id,
                        RcaJob.requested_by == ctx.user_id,
                        RcaJob.status != JobStatus.DELETED.value,
                    )
                )
                if status_filter is not None:
                    stmt = stmt.where(RcaJob.status == status_filter.value)

                cursor_created_at, cursor_job_id = _decode_cursor(cursor)
                if cursor_created_at is not None and cursor_job_id is not None:
                    stmt = stmt.where(
                        or_(
                            RcaJob.created_at < cursor_created_at,
                            and_(RcaJob.created_at == cursor_created_at, RcaJob.job_id < cursor_job_id),
                        )
                    )

                stmt = stmt.order_by(RcaJob.created_at.desc(), RcaJob.job_id.desc()).limit(page_size + 1)
                rows = db.scalars(stmt).all()
                page = rows[:page_size]

                next_cursor = None
                if len(rows) > page_size:
                    tail = page[-1]
                    next_cursor = _encode_cursor(created_at=tail.created_at, job_id=tail.job_id)
                return [_to_view(item) for item in page], next_cursor

        return await asyncio.to_thread(_list)

    async def get_job(self, *, job_id: str, ctx: InternalContext) -> JobView:
        def _get() -> JobView:
            with get_db_session() as db:
                row = db.get(RcaJob, job_id)
                if row is None or row.status == JobStatus.DELETED.value:
                    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="RCA job not found")
                if row.tenant_id != ctx.tenant_id:
                    raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Access denied for this RCA job")
                return _to_view(row)

        return await asyncio.to_thread(_get)

    async def get_job_result(self, *, job_id: str, ctx: InternalContext) -> tuple[JobView, Optional[JSONDict]]:
        def _get() -> tuple[JobView, Optional[JSONDict]]:
            with get_db_session() as db:
                row = db.get(RcaJob, job_id)
                if row is None or row.status == JobStatus.DELETED.value:
                    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="RCA job not found")
                if row.tenant_id != ctx.tenant_id:
                    raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Access denied for this RCA job")
                if row.status != JobStatus.COMPLETED.value:
                    raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="RCA job is not completed yet")
                report = db.get(RcaReport, row.report_id)
                if report is None or report.result_payload is None:
                    raise HTTPException(status_code=status.HTTP_410_GONE, detail="RCA job result has expired")
                return _to_view(row), report.result_payload

        return await asyncio.to_thread(_get)

    async def get_report(self, *, report_id: str, ctx: InternalContext) -> tuple[JobView, Optional[JSONDict]]:
        def _get() -> tuple[JobView, Optional[JSONDict]]:
            with get_db_session() as db:
                report = db.get(RcaReport, report_id)
                if report is None:
                    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="RCA report not found")
                if report.tenant_id != ctx.tenant_id:
                    raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Access denied for this RCA report")
                row = db.get(RcaJob, report.job_id)
                if row is None or row.status == JobStatus.DELETED.value:
                    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="RCA report not found")
                if report.result_payload is None:
                    raise HTTPException(status_code=status.HTTP_410_GONE, detail="RCA report payload has expired")
                return _to_view(row), report.result_payload

        return await asyncio.to_thread(_get)

    async def delete_report(self, *, report_id: str, ctx: InternalContext) -> None:
        task_to_cancel: Optional[asyncio.Task[None]] = None

        def _delete() -> str:
            with get_db_session() as db:
                report = db.get(RcaReport, report_id)
                if report is None:
                    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="RCA report not found")
                if report.tenant_id != ctx.tenant_id:
                    raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Access denied for this RCA report")
                row = db.get(RcaJob, report.job_id)
                if row is None:
                    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="RCA report not found")
                if row.requested_by != ctx.user_id and not ctx.is_superuser:
                    raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Only the report owner can delete this report")

                now = _utcnow()
                row.status = JobStatus.DELETED.value
                row.deleted_at = now
                row.delete_requested_by = ctx.user_id
                row.finished_at = row.finished_at or now
                row.error = row.error or "Deleted by report owner"
                report.result_payload = None
                return row.job_id

        job_id = await asyncio.to_thread(_delete)
        async with self._lock:
            task_to_cancel = self._tasks.get(job_id)
        if task_to_cancel is not None and not task_to_cancel.done():
            task_to_cancel.cancel()


rca_job_service = RcaJobService()
