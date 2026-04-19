"""
Copyright (c) 2026 Stefan Kumarasinghe.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. See http://www.apache.org/licenses/LICENSE-2.0 for details.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from fastapi import HTTPException

from api.requests import AnalyzeJobCreateRequest
from api.responses import JobStatus
from api.responses.jobs import AnalyzeJobSummary as JobView
from api.routes import jobs as jobs_route
from services.security_service import InternalContext


def _ctx() -> InternalContext:
    return InternalContext(
        tenant_id="tenant-a",
        org_id="tenant-a",
        user_id="user-1",
        username="alice",
        permissions=["create:rca", "read:rca", "delete:rca"],
        group_ids=[],
        role="user",
        is_superuser=False,
    )


def _job_view(status: JobStatus = JobStatus.QUEUED) -> JobView:
    now = datetime.now(UTC)
    return JobView(
        job_id="job-1",
        report_id="report-1",
        status=status,
        created_at=now,
        started_at=now,
        finished_at=now,
        duration_ms=25,
        error=None,
        summary_preview="summary",
        tenant_id="tenant-a",
        requested_by="user-1",
    )


def test_required_context_requires_authentication(monkeypatch):
    monkeypatch.setattr(jobs_route, "get_internal_context", lambda: None)
    with pytest.raises(HTTPException) as exc:
        jobs_route._required_context()
    assert exc.value.status_code == 401


def test_required_context_and_require_permission_delegate(monkeypatch):
    calls = []
    monkeypatch.setattr(jobs_route, "ensure_permission", lambda name: calls.append(name))
    monkeypatch.setattr(jobs_route, "get_internal_context", _ctx)

    jobs_route._require_permission("read:rca")
    ctx = jobs_route._required_context()

    assert calls == ["read:rca"]
    assert ctx.tenant_id == "tenant-a"


def test_summary_maps_job_view():
    summary = jobs_route._summary(_job_view(JobStatus.RUNNING))
    assert summary.job_id == "job-1"
    assert summary.status == JobStatus.RUNNING
    assert summary.summary_preview == "summary"


@pytest.mark.asyncio
async def test_create_job_route(monkeypatch):
    monkeypatch.setattr(jobs_route, "_require_permission", lambda name: None)
    monkeypatch.setattr(jobs_route, "_required_context", _ctx)

    async def fake_create_job(payload, ctx):
        assert ctx.tenant_id == "tenant-a"
        assert payload.tenant_id == "tenant-request"
        return _job_view(JobStatus.QUEUED)

    monkeypatch.setattr(jobs_route.rca_job_service, "create_job", fake_create_job)
    payload = AnalyzeJobCreateRequest(tenant_id="tenant-request", start=10, end=20)

    response = await jobs_route.create_job(payload)

    assert response.job_id == "job-1"
    assert response.status == JobStatus.QUEUED
    assert response.tenant_id == "tenant-a"


@pytest.mark.asyncio
async def test_create_job_route_preserves_config_yaml(monkeypatch):
    monkeypatch.setattr(jobs_route, "_require_permission", lambda name: None)
    monkeypatch.setattr(jobs_route, "_required_context", _ctx)

    async def fake_create_job(payload, ctx):
        assert ctx.tenant_id == "tenant-a"
        assert "settings:" in str(payload.config_yaml)
        return _job_view(JobStatus.QUEUED)

    monkeypatch.setattr(jobs_route.rca_job_service, "create_job", fake_create_job)
    payload = AnalyzeJobCreateRequest(
        tenant_id="tenant-request",
        start=10,
        end=20,
        config_yaml="version: 1\nsettings:\n  mad_threshold: 8.0\n",
    )

    response = await jobs_route.create_job(payload)

    assert response.job_id == "job-1"


@pytest.mark.asyncio
async def test_list_jobs_route(monkeypatch):
    monkeypatch.setattr(jobs_route, "_require_permission", lambda name: None)
    monkeypatch.setattr(jobs_route, "_required_context", _ctx)

    async def fake_list_jobs(ctx, status_filter, limit, cursor):
        assert ctx.user_id == "user-1"
        assert status_filter == JobStatus.COMPLETED
        assert limit == 5
        assert cursor == "cursor-1"
        return [_job_view(JobStatus.COMPLETED)], "cursor-2"

    monkeypatch.setattr(jobs_route.rca_job_service, "list_jobs", fake_list_jobs)

    response = await jobs_route.list_jobs(status_filter=JobStatus.COMPLETED, limit=5, cursor="cursor-1")

    assert response.next_cursor == "cursor-2"
    assert len(response.items) == 1
    assert response.items[0].status == JobStatus.COMPLETED


@pytest.mark.asyncio
async def test_get_job_routes(monkeypatch):
    monkeypatch.setattr(jobs_route, "_require_permission", lambda name: None)
    monkeypatch.setattr(jobs_route, "_required_context", _ctx)

    async def fake_get_job(job_id, ctx):
        assert job_id == "job-1"
        assert ctx.tenant_id == "tenant-a"
        return _job_view(JobStatus.RUNNING)

    async def fake_get_job_result(job_id, ctx):
        assert job_id == "job-1"
        return _job_view(JobStatus.COMPLETED), {"report": True}

    async def fake_get_report(report_id, ctx):
        assert report_id == "report-1"
        return _job_view(JobStatus.COMPLETED), {"report_id": report_id}

    async def fake_delete_report(report_id, ctx):
        assert report_id == "report-1"
        assert ctx.user_id == "user-1"
        return None

    monkeypatch.setattr(jobs_route.rca_job_service, "get_job", fake_get_job)
    monkeypatch.setattr(jobs_route.rca_job_service, "get_job_result", fake_get_job_result)
    monkeypatch.setattr(jobs_route.rca_job_service, "get_report", fake_get_report)
    monkeypatch.setattr(jobs_route.rca_job_service, "delete_report", fake_delete_report)

    job = await jobs_route.get_job("job-1")
    job_result = await jobs_route.get_job_result("job-1")
    report = await jobs_route.get_report("report-1")
    deleted = await jobs_route.delete_report("report-1")

    assert job.status == JobStatus.RUNNING
    assert job_result.result == {"report": True}
    assert report.result == {"report_id": "report-1"}
    assert deleted.report_id == "report-1"
    assert deleted.deleted is True
