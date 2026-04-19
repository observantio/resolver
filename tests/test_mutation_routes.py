"""
Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from fastapi import HTTPException

from api.requests import AnalyzeJobCreateRequest, AnalyzeRequest, TopologyRequest
from api.responses import JobStatus
from api.responses.jobs import AnalyzeJobSummary as JobView
from api.routes import analyze as analyze_route
from api.routes import jobs as jobs_route
from api.routes import topology as topology_route
from services.security_service import InternalContext


def _ctx() -> InternalContext:
    return InternalContext(
        tenant_id="tenant-a",
        org_id="tenant-a",
        user_id="u1",
        username="alice",
        permissions=["create:rca", "read:rca", "delete:rca"],
        group_ids=[],
        role="user",
        is_superuser=False,
    )


def _job(status: JobStatus = JobStatus.COMPLETED) -> JobView:
    now = datetime.now(UTC)
    return JobView(
        job_id="job-1",
        report_id="rep-1",
        status=status,
        created_at=now,
        started_at=now,
        finished_at=now,
        duration_ms=5,
        error="transient",
        summary_preview="ok",
        tenant_id="tenant-a",
        requested_by="u1",
    )


@pytest.mark.asyncio
async def test_analyze_and_template_routes(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_run(req: AnalyzeRequest):
        return {
            "tenant_id": req.tenant_id,
            "start": req.start,
            "end": req.end,
            "duration_seconds": req.end - req.start,
            "metric_anomalies": [],
            "log_bursts": [],
            "log_patterns": [],
            "service_latency": [],
            "error_propagation": [],
            "slo_alerts": [],
            "root_causes": [],
            "ranked_causes": [],
            "change_points": [],
            "log_metric_links": [],
            "forecasts": [],
            "degradation_signals": [],
            "anomaly_clusters": [],
            "granger_results": [],
            "bayesian_scores": [],
            "analysis_warnings": [],
            "overall_severity": "low",
            "summary": "ok",
            "quality": {
                "anomaly_density": {},
                "suppression_counts": {},
                "gating_profile": None,
                "confidence_calibration_version": None,
            },
        }

    monkeypatch.setattr(analyze_route, "run_analysis", fake_run)
    monkeypatch.setattr(
        analyze_route.analysis_config_service,
        "template_response",
        lambda: {
            "version": 1,
            "defaults": {"request": {"step": "15s"}},
            "template_yaml": "version: 1",
            "file_name": "resolver-rca-defaults.yaml",
        },
    )

    out = await analyze_route.analyze(AnalyzeRequest(tenant_id="tenant-a", start=1, end=2))
    template = await analyze_route.analyze_config_template()

    assert out["tenant_id"] == "tenant-a"
    assert template.file_name == "resolver-rca-defaults.yaml"


@pytest.mark.asyncio
async def test_jobs_routes(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(jobs_route, "ensure_permission", lambda _name: None)
    monkeypatch.setattr(jobs_route, "get_internal_context", _ctx)

    async def fake_create(payload, ctx):
        assert payload.tenant_id == "tenant-a"
        assert ctx.user_id == "u1"
        return _job(JobStatus.QUEUED)

    async def fake_list(ctx, status_filter, limit, cursor):
        assert status_filter == JobStatus.COMPLETED
        assert limit == 5
        assert cursor == "c1"
        return [_job(JobStatus.COMPLETED)], "c2"

    async def fake_get(job_id, ctx):
        assert job_id == "job-1"
        assert ctx.tenant_id == "tenant-a"
        return _job()

    async def fake_result(job_id, ctx):
        assert job_id == "job-1"
        assert ctx.tenant_id == "tenant-a"
        return _job(), {"report": True}

    async def fake_report(report_id, ctx):
        assert report_id == "rep-1"
        assert ctx.user_id == "u1"
        return _job(), {"report_id": report_id}

    async def fake_delete(report_id, ctx):
        assert report_id == "rep-1"
        assert ctx.user_id == "u1"
        return None

    monkeypatch.setattr(jobs_route.rca_job_service, "create_job", fake_create)
    monkeypatch.setattr(jobs_route.rca_job_service, "list_jobs", fake_list)
    monkeypatch.setattr(jobs_route.rca_job_service, "get_job", fake_get)
    monkeypatch.setattr(jobs_route.rca_job_service, "get_job_result", fake_result)
    monkeypatch.setattr(jobs_route.rca_job_service, "get_report", fake_report)
    monkeypatch.setattr(jobs_route.rca_job_service, "delete_report", fake_delete)

    created = await jobs_route.create_job(AnalyzeJobCreateRequest(tenant_id="tenant-a", start=1, end=2))
    listed = await jobs_route.list_jobs(status_filter=JobStatus.COMPLETED, limit=5, cursor="c1")
    summary = await jobs_route.get_job("job-1")
    result = await jobs_route.get_job_result("job-1")
    report = await jobs_route.get_report("rep-1")
    deleted = await jobs_route.delete_report("rep-1")

    assert created.status == JobStatus.QUEUED
    assert listed.next_cursor == "c2"
    assert listed.items[0].summary_preview == "ok"
    assert summary.job_id == "job-1"
    assert summary.summary_preview == "ok"
    assert result.result == {"report": True}
    assert report.result == {"report_id": "rep-1"}
    assert deleted.deleted is True
    assert "started_at" in listed.items[0].model_fields_set
    assert "finished_at" in listed.items[0].model_fields_set
    assert "duration_ms" in listed.items[0].model_fields_set
    assert listed.items[0].error == "transient"
    assert "summary_preview" in listed.items[0].model_fields_set
    assert summary.started_at is not None
    assert summary.finished_at is not None
    assert summary.duration_ms == 5
    assert summary.error == "transient"
    assert "started_at" in summary.model_fields_set
    assert "finished_at" in summary.model_fields_set
    assert "duration_ms" in summary.model_fields_set
    assert "error" in summary.model_fields_set
    assert "summary_preview" in summary.model_fields_set


def test_jobs_required_context_raises_when_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(jobs_route, "get_internal_context", lambda: None)
    with pytest.raises(HTTPException) as exc:
        jobs_route._required_context()
    assert exc.value.status_code == 401
    assert exc.value.detail == "Missing internal context"

def test_jobs_require_permission_forwards_name(monkeypatch: pytest.MonkeyPatch) -> None:
    seen = {}

    def fake_ensure_permission(name: str) -> None:
        seen["name"] = name

    monkeypatch.setattr(jobs_route, "ensure_permission", fake_ensure_permission)
    jobs_route._require_permission("read:rca")
    assert seen["name"] == "read:rca"

@pytest.mark.asyncio
async def test_topology_route(monkeypatch: pytest.MonkeyPatch) -> None:
    class DummyProvider:
        async def query_traces(self, filters, start, end):
            assert filters == {}
            assert start == 1
            assert end == 100
            return {
                "traces": [
                    {
                        "rootServiceName": "checkout",
                        "spanSet": {
                            "spans": [
                                {
                                    "attributes": [
                                        {"key": "service.name", "value": {"stringValue": "checkout"}},
                                        {"key": "peer.service", "value": {"stringValue": "payments"}},
                                    ]
                                }
                            ]
                        },
                    }
                ]
            }

    monkeypatch.setattr(topology_route, "enforce_request_tenant", lambda req: req)
    monkeypatch.setattr(topology_route, "safe_call", lambda coro: coro)
    monkeypatch.setattr(topology_route, "get_provider", lambda _tid: DummyProvider())

    out = await topology_route.blast_radius(
        TopologyRequest(tenant_id="tenant-a", start=1, end=100, root_service="checkout", max_depth=2)
    )
    assert out["root_service"] == "checkout"
    assert "payments" in out["affected_downstream"]
