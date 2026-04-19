"""
Copyright (c) 2026 Stefan Kumarasinghe.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. See http://www.apache.org/licenses/LICENSE-2.0 for details.
"""

from __future__ import annotations

import importlib
import types

import httpx
import pytest
from fastapi.routing import APIRoute

import api.responses as response_exports
import main as app_main
from api.responses import JobStatus
from api.routes import health as health_route
from config import LOGS_BACKEND_LOKI, METRICS_BACKEND_MIMIR, TRACES_BACKEND_TEMPO
from services import analyze_service


@pytest.mark.asyncio
async def test_health_route_reports_store_backend(monkeypatch):
    calls = []

    async def fake_get_redis():
        calls.append("redis")
        return object()

    monkeypatch.setattr(health_route, "get_redis", fake_get_redis)
    monkeypatch.setattr(health_route, "is_using_fallback", lambda: False)
    assert await health_route.health() == {"status": "ok", "store": "redis"}
    monkeypatch.setattr(health_route, "is_using_fallback", lambda: True)
    assert await health_route.health() == {"status": "ok", "store": "fallback"}
    assert calls == ["redis", "redis"]


@pytest.mark.asyncio
async def test_run_analysis_uses_tenant_scoped_provider(monkeypatch):
    calls = {}

    def fake_enforce_request_tenant(req):
        calls["enforced"] = req.tenant_id
        return req.model_copy(update={"tenant_id": "tenant-from-context"})

    def fake_get_provider(tenant_id):
        calls["provider_tenant"] = tenant_id
        return "provider"

    async def fake_run(provider, req):
        calls["provider"] = provider
        calls["tenant"] = req.tenant_id
        return {"ok": True, "tenant": req.tenant_id}

    monkeypatch.setattr(analyze_service, "enforce_request_tenant", fake_enforce_request_tenant)
    monkeypatch.setattr(analyze_service, "get_provider", fake_get_provider)
    monkeypatch.setattr(analyze_service, "run", fake_run)

    req = importlib.import_module("api.requests").AnalyzeRequest(tenant_id="tenant-request", start=10, end=20)
    result = await analyze_service.run_analysis(req)

    assert result == {"ok": True, "tenant": "tenant-from-context"}
    assert calls == {
        "enforced": "tenant-request",
        "provider_tenant": "tenant-from-context",
        "provider": "provider",
        "tenant": "tenant-from-context",
    }


def test_response_exports_dynamic_lookup_and_missing_attr():
    assert response_exports.JobStatus is JobStatus
    assert response_exports.AnalyzeJobSummary.__name__ == "AnalyzeJobSummary"
    with pytest.raises(AttributeError):
        response_exports.DoesNotExist


@pytest.mark.asyncio
async def test_wait_for_success_and_timeout(monkeypatch):
    class FakeResponse:
        def __init__(self, status_code):
            self.status_code = status_code

    class FakeAsyncClient:
        def __init__(self):
            self.calls = 0

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        async def get(self, url, headers=None, timeout=3.0):
            self.calls += 1
            if self.calls == 1:
                raise httpx.RequestError("not yet")
            if self.calls == 2:
                return FakeResponse(500)
            return FakeResponse(200)

    client = FakeAsyncClient()
    monkeypatch.setattr(app_main.httpx, "AsyncClient", lambda: client)

    async def fake_sleep(_seconds):
        return None

    monkeypatch.setattr(app_main.asyncio, "sleep", fake_sleep)
    await app_main.wait_for("loki", "http://loki", 1)

    class _TimeoutClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        async def get(self, url, headers=None, timeout=3.0):
            raise httpx.RequestError("down")

    monkeypatch.setattr(app_main.httpx, "AsyncClient", _TimeoutClient)
    with pytest.raises(app_main.BackendStartupTimeout):
        await app_main.wait_for("tempo", "http://tempo", 0)


@pytest.mark.asyncio
async def test_main_background_helpers(monkeypatch):
    ready_settings = types.SimpleNamespace(
        logs_backend=LOGS_BACKEND_LOKI,
        metrics_backend=METRICS_BACKEND_MIMIR,
        traces_backend=TRACES_BACKEND_TEMPO,
        loki_url="http://loki",
        mimir_url="http://mimir",
        tempo_url="http://tempo",
        startup_timeout=3,
    )

    async def fake_wait_for(name, url, timeout, headers=None, accept_status=(200,)):
        return None

    monkeypatch.setattr(app_main, "wait_for", fake_wait_for)
    app_main._BACKEND_READY = False
    app_main._BACKEND_STATUS = {}
    await app_main._wait_for_all_bg(ready_settings, "tenant-a")
    assert app_main._BACKEND_READY is True
    assert set(app_main._BACKEND_STATUS.values()) == {"ready"}


@pytest.mark.asyncio
async def test_ready_endpoint_success_payload():
    app_main._BACKEND_READY = True
    app_main._BACKEND_STATUS = {"tempo": "ready"}
    response = await app_main.ready()
    assert response.status_code == 200
    assert b'"ready":true' in response.body


def test_openapi_server_and_operation_id_helpers(monkeypatch):
    monkeypatch.setattr(app_main.settings, "ssl_enabled", False)
    monkeypatch.setattr(app_main.settings, "host", "127.0.0.1")
    monkeypatch.setattr(app_main.settings, "port", 4322)
    servers = app_main._openapi_servers()
    assert servers[0] == {"url": "/"}
    assert servers[1]["url"].startswith("http://127.0.0.1:4322")

    async def _endpoint() -> None:
        return None

    route = APIRoute("/jobs", _endpoint, methods=["GET"], name="list_jobs")
    assert app_main._generate_operation_id(route) == "list_jobs"
