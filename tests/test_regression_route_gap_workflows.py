"""
Regression workflow tests for route coverage gaps in Resolver.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from api.requests import TopologyRequest
from api.responses import JobStatus
from api.routes import analyze as analyze_route
from api.routes import jobs as jobs_route
from api.routes import topology as topology_route
from services.security_service import InternalContext


def _ctx() -> InternalContext:
    return InternalContext(
        tenant_id="tenant-a",
        org_id="tenant-a",
        user_id="user-1",
        username="alice",
        permissions=["read:rca", "delete:rca"],
        group_ids=[],
        role="user",
        is_superuser=False,
    )


def _job(status: JobStatus = JobStatus.COMPLETED):
    now = datetime.now(UTC)
    return type(
        "JobStub",
        (),
        {
            "job_id": "job-1",
            "report_id": "report-1",
            "status": status,
            "created_at": now,
            "started_at": now,
            "finished_at": now,
            "duration_ms": 20,
            "error": None,
            "summary_preview": "ok",
            "tenant_id": "tenant-a",
            "requested_by": "user-1",
        },
    )()


@pytest.mark.asyncio
async def test_analyze_config_template_route_returns_service_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        analyze_route.analysis_config_service,
        "template_response",
        lambda: {
            "version": 2,
            "defaults": {"request": {"step": "30s"}},
            "template_yaml": "version: 2\nrequest:\n  step: 30s\n",
            "file_name": "resolver-rca-v2.yaml",
        },
    )

    result = await analyze_route.analyze_config_template()

    assert result.version == 2
    assert result.defaults["request"]["step"] == "30s"
    assert result.file_name == "resolver-rca-v2.yaml"


@pytest.mark.asyncio
async def test_get_report_enforces_read_permission_and_context(monkeypatch: pytest.MonkeyPatch) -> None:
    permission_calls: list[str] = []
    captured: dict[str, object] = {}

    monkeypatch.setattr(jobs_route, "ensure_permission", lambda name: permission_calls.append(name))
    monkeypatch.setattr(jobs_route, "get_internal_context", _ctx)

    async def _get_report(report_id: str, ctx: InternalContext):
        captured["report_id"] = report_id
        captured["ctx"] = ctx
        return _job(), {"report_id": report_id}

    monkeypatch.setattr(jobs_route.rca_job_service, "get_report", _get_report)

    result = await jobs_route.get_report("report-42")

    assert permission_calls == ["read:rca"]
    assert captured["report_id"] == "report-42"
    assert isinstance(captured["ctx"], InternalContext)
    assert result.report_id == "report-1"
    assert result.result == {"report_id": "report-42"}


@pytest.mark.asyncio
async def test_delete_report_enforces_delete_permission_and_context(monkeypatch: pytest.MonkeyPatch) -> None:
    permission_calls: list[str] = []
    captured: dict[str, object] = {}

    monkeypatch.setattr(jobs_route, "ensure_permission", lambda name: permission_calls.append(name))
    monkeypatch.setattr(jobs_route, "get_internal_context", _ctx)

    async def _delete_report(report_id: str, ctx: InternalContext):
        captured["report_id"] = report_id
        captured["ctx"] = ctx

    monkeypatch.setattr(jobs_route.rca_job_service, "delete_report", _delete_report)

    result = await jobs_route.delete_report("report-99")

    assert permission_calls == ["delete:rca"]
    assert captured["report_id"] == "report-99"
    assert isinstance(captured["ctx"], InternalContext)
    assert result.report_id == "report-99"
    assert result.deleted is True


@pytest.mark.asyncio
async def test_blast_radius_enforces_tenant_and_queries_trace_provider(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    class _Provider:
        async def query_traces(self, filters, start, end, limit=None):
            captured["filters"] = filters
            captured["start"] = start
            captured["end"] = end
            captured["limit"] = limit
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

    provider = _Provider()

    def _enforce_request_tenant(req: TopologyRequest) -> TopologyRequest:
        return req.model_copy(update={"tenant_id": "tenant-enforced"})

    async def _safe_call(awaitable):
        return await awaitable

    def _get_provider(tenant_id: str):
        captured["tenant_id"] = tenant_id
        return provider

    monkeypatch.setattr(topology_route, "enforce_request_tenant", _enforce_request_tenant)
    monkeypatch.setattr(topology_route, "safe_call", _safe_call)
    monkeypatch.setattr(topology_route, "get_provider", _get_provider)

    req = TopologyRequest(
        tenant_id="tenant-spoofed",
        start=10,
        end=20,
        root_service="checkout",
        max_depth=3,
    )

    result = await topology_route.blast_radius(req)

    assert captured["tenant_id"] == "tenant-enforced"
    assert captured["filters"] == {}
    assert captured["start"] == 10
    assert captured["end"] == 20
    assert "payments" in result["affected_downstream"]
