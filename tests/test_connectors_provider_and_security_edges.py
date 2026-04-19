"""
Copyright (c) 2026 Stefan Kumarasinghe.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
"""

from __future__ import annotations

from types import SimpleNamespace

import httpx
import jwt
import pytest
from fastapi import HTTPException
from pydantic import BaseModel

import services.security_service as security_service
from connectors.loki import LokiConnector
from connectors.mimir import MimirConnector
from connectors.tempo import TempoConnector
from datasources.base import BaseConnector
from datasources.exceptions import DataSourceUnavailable, InvalidQuery, QueryTimeout
from datasources.helpers import FetchRequestOptions, fetch_json, fetch_text
from datasources.provider import DataSourceProvider


class _DummyConnector(BaseConnector):
    pass


class _Response:
    def __init__(self, payload=None, *, text="", status_code=200):
        self._payload = payload if payload is not None else {}
        self.text = text
        self.status_code = status_code

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            request = httpx.Request("GET", "https://example")
            response = httpx.Response(self.status_code, request=request, text=self.text)
            raise httpx.HTTPStatusError("bad", request=request, response=response)


class _AsyncClient:
    def __init__(self, response=None, error=None):
        self.response = response or _Response({})
        self.error = error
        self.closed = False
        self.calls = []

    async def get(self, url, params=None, headers=None):
        self.calls.append((url, params, headers))
        if self.error:
            raise self.error
        return self.response

    async def aclose(self):
        self.closed = True


@pytest.mark.asyncio
async def test_datasource_helpers_and_provider(monkeypatch):
    connector = _DummyConnector("tenant", "https://example/", timeout=3, headers={"A": "b"})
    with pytest.raises(NotImplementedError):
        _ = connector.health_url
    connector.health_path = "/health"
    assert connector.health_url == "https://example/health"
    assert connector._headers() == {"A": "b", "X-Scope-OrgID": "tenant"}
    await connector.aclose()
    assert connector.client.is_closed is True

    async_client = _AsyncClient(_Response({"ok": True}))
    assert await fetch_json("https://api", options=FetchRequestOptions(client=async_client)) == {"ok": True}
    assert (
        await fetch_text("https://api", options=FetchRequestOptions(client=_AsyncClient(_Response(text="body"))))
        == "body"
    )
    assert await fetch_json("https://api", options=FetchRequestOptions(client=_AsyncClient(_Response([1, 2, 3])))) == {}

    with pytest.raises(InvalidQuery):
        await fetch_json("https://api", options=FetchRequestOptions(client=_AsyncClient(_Response(status_code=400, text="bad"))))
    with pytest.raises(QueryTimeout):
        await fetch_json("https://api", options=FetchRequestOptions(client=_AsyncClient(error=httpx.TimeoutException("timeout"))))
    with pytest.raises(DataSourceUnavailable):
        await fetch_json(
            "https://api",
            options=FetchRequestOptions(
                client=_AsyncClient(error=httpx.RequestError("down", request=httpx.Request("GET", "https://api")))
            ),
        )

    with pytest.raises(InvalidQuery):
        await fetch_text("https://api", options=FetchRequestOptions(client=_AsyncClient(_Response(status_code=500, text="bad"))))
    with pytest.raises(QueryTimeout):
        await fetch_text("https://api", options=FetchRequestOptions(client=_AsyncClient(error=httpx.TimeoutException("timeout"))))
    with pytest.raises(DataSourceUnavailable):
        await fetch_text(
            "https://api",
            options=FetchRequestOptions(
                client=_AsyncClient(error=httpx.RequestError("down", request=httpx.Request("GET", "https://api")))
            ),
        )

    async def _logs_query_range(**kwargs):
        return {"logs": kwargs}

    async def _metrics_query_range(**kwargs):
        return {"metrics": kwargs}

    async def _traces_query_range(**kwargs):
        return {"traces": kwargs}

    logs = SimpleNamespace(query_range=_logs_query_range, aclose=lambda: _awaitable())
    metrics = SimpleNamespace(query_range=_metrics_query_range, aclose=lambda: _awaitable())
    traces = SimpleNamespace(query_range=_traces_query_range, aclose=lambda: _awaitable())
    monkeypatch.setattr("datasources.provider.DataSourceFactory.create_logs", lambda settings, tenant_id: logs)
    monkeypatch.setattr("datasources.provider.DataSourceFactory.create_metrics", lambda settings, tenant_id: metrics)
    monkeypatch.setattr("datasources.provider.DataSourceFactory.create_traces", lambda settings, tenant_id: traces)
    provider = DataSourceProvider("tenant", SimpleNamespace())
    assert await provider.query_logs("{job='x'}", 1, 2, limit=3) == {
        "logs": {"query": "{job='x'}", "start": 1, "end": 2, "limit": 3}
    }
    assert await provider.query_metrics("up", 1, 2, step="60s") == {
        "metrics": {"query": "up", "start": 1, "end": 2, "step": "60s"}
    }
    assert await provider.query_traces({"service.name": "api"}, 1, 2, limit=4) == {
        "traces": {"filters": {"service.name": "api"}, "start": 1, "end": 2, "limit": 4}
    }
    assert await provider.query_logs("{job='x'}", 1, 2, limit="NaN") == {
        "logs": {"query": "{job='x'}", "start": 1, "end": 2, "limit": None}
    }
    assert await provider.query_traces({"service.name": "api"}, 1, 2, limit=object()) == {
        "traces": {"filters": {"service.name": "api"}, "start": 1, "end": 2, "limit": None}
    }
    assert await provider.query_logs("{job='x'}", 1, 2) == {
        "logs": {"query": "{job='x'}", "start": 1, "end": 2, "limit": None}
    }
    with pytest.raises(TypeError, match="step is required"):
        await provider.query_metrics("up", 1, 2)
    await provider.aclose()


async def _awaitable():
    return None


@pytest.mark.asyncio
async def test_specific_connectors_build_expected_requests(monkeypatch):
    recorded = []

    async def fake_query_backend_json(connector, path, params, messages=None):
        recorded.append((connector.__class__.__name__, path, params, messages))
        return {"path": path, "params": params}

    async def fake_fetch_text(url, options=None, messages=None):
        recorded.append(("fetch_text", url, options, messages))
        return "metrics"

    monkeypatch.setattr("connectors.loki.query_backend_json", fake_query_backend_json)
    monkeypatch.setattr("connectors.mimir.query_backend_json", fake_query_backend_json)
    monkeypatch.setattr("connectors.tempo.query_backend_json", fake_query_backend_json)
    monkeypatch.setattr("connectors.mimir.fetch_text", fake_fetch_text)

    loki = LokiConnector("https://loki", "tenant")
    mimir = MimirConnector("https://mimir", "tenant")
    tempo = TempoConnector("https://tempo", "tenant")

    assert LokiConnector._normalize_query("") == '{service=~".+"}'
    assert await loki.query_range('{app=~".*"}', 1, 2, limit=100) == {
        "path": "/loki/api/v1/query_range",
        "params": {"query": '{app=~".+"}', "start": 1, "end": 2, "limit": 100},
    }
    assert await mimir.scrape() == "metrics"
    assert await mimir.query_range("up", 1, 2, step="60s") == {
        "path": "/prometheus/api/v1/query_range",
        "params": {"query": "up", "start": 1, "end": 2, "step": "60s"},
    }
    assert await tempo.query_range({"service.name": "api"}, 1, 2, limit=5) == {
        "path": "/api/search",
        "params": {"start": 1, "end": 2, "service.name": "api", "limit": 5},
    }


@pytest.mark.asyncio
async def test_mimir_query_range_compatibility_errors(monkeypatch):
    async def _raising_query_backend_json(*args, **kwargs):
        raise TypeError("boom")

    monkeypatch.setattr("connectors.mimir.query_backend_json", _raising_query_backend_json)
    connector = MimirConnector("https://mimir", "tenant")
    with pytest.raises(TypeError, match="boom"):
        await connector.query_range("up", 1, 2, step="60s")
    with pytest.raises(TypeError, match="step is required"):
        await connector.query_range("up", 1, 2)


def _set_security_defaults() -> None:
    security_service.settings.expected_service_token = "internal-service-token"
    security_service.settings.context_verify_key = "very-secret-signing-key-with-32-bytes"
    security_service.settings.context_issuer = "watchdog-main"
    security_service.settings.context_audience = "resolver"
    security_service.settings.context_algorithms = "HS256"
    security_service.settings.context_replay_ttl_seconds = 180
    with security_service._jti_seen_lock:
        security_service._jti_seen_cache.clear()


def _context_headers(payload: dict[str, object]) -> dict[str, str]:
    token = jwt.encode(payload, security_service.settings.context_verify_key, algorithm="HS256")
    return {"x-service-token": security_service.settings.expected_service_token, "authorization": f"Bearer {token}"}


def test_security_service_remaining_edges(monkeypatch):
    _set_security_defaults()
    assert security_service._context_algorithms() == ["HS256"]
    security_service.settings.context_algorithms = "HS256,BAD"
    with pytest.raises(HTTPException):
        security_service._context_algorithms()
    security_service.settings.context_algorithms = "HS256"

    now = 1000.0
    monkeypatch.setattr(security_service.time, "monotonic", lambda: now)
    with security_service._jti_seen_lock:
        security_service._jti_seen_cache.update({"old": 700.0})
    security_service._assert_jti_not_replayed("new")
    with pytest.raises(HTTPException):
        security_service._assert_jti_not_replayed("new")

    with pytest.raises(HTTPException):
        security_service._parse_bearer(None)
    with pytest.raises(HTTPException):
        security_service._parse_bearer("Basic abc")
    assert security_service._parse_bearer("Bearer abc") == "abc"
    assert security_service._string_list([" a ", "", None]) == ["a", "None"]
    assert security_service._required_int_claim({"x": 1.2}, "x") == 1
    assert security_service._required_int_claim({"x": "2"}, "x") == 2
    with pytest.raises(TypeError):
        security_service._required_int_claim({"x": True}, "x")

    security_service.settings.context_verify_key = ""
    with pytest.raises(HTTPException):
        security_service._decode_context_token("x")
    security_service.settings.context_verify_key = "very-secret-signing-key-with-32-bytes"
    with pytest.raises(HTTPException):
        security_service._decode_context_token("bad-token")

    expired = jwt.encode(
        {"iss": "watchdog-main", "aud": "resolver", "iat": 1, "exp": 1, "jti": "jti-1", "tenant_id": "tenant"},
        security_service.settings.context_verify_key,
        algorithm="HS256",
    )
    with pytest.raises(HTTPException):
        security_service._decode_context_token(expired)

    token = jwt.encode(
        {"iss": "watchdog-main", "aud": "resolver", "iat": 2, "exp": 1, "jti": "jti-2", "tenant_id": "tenant"},
        security_service.settings.context_verify_key,
        algorithm="HS256",
    )
    with pytest.raises(HTTPException):
        security_service._decode_context_token(token)

    with pytest.raises(HTTPException):
        security_service._build_context({})
    ctx = security_service._build_context(
        {
            "tenant_id": "tenant",
            "org_id": "org",
            "user_id": "u1",
            "username": "alice",
            "permissions": ["read"],
            "group_ids": ["g1"],
            "role": "admin",
            "is_superuser": True,
        }
    )
    assert ctx.tenant_id == "tenant"

    token_var = security_service.set_internal_context(ctx)
    try:
        assert security_service.get_context_tenant() == "tenant"
        assert security_service.ensure_permission("read") == ctx
    finally:
        security_service.reset_internal_context(token_var)
    assert security_service.get_context_tenant("fallback") == "fallback"
    with pytest.raises(HTTPException):
        security_service.get_context_tenant()
    with pytest.raises(HTTPException):
        security_service.ensure_permission("read")
    token_var = security_service.set_internal_context(
        security_service.InternalContext("tenant", "org", "u1", "alice", [], [], "user", False)
    )
    try:
        with pytest.raises(HTTPException):
            security_service.ensure_permission("write")
    finally:
        security_service.reset_internal_context(token_var)
    dep = security_service.require_permission_dependency("read")
    token_var = security_service.set_internal_context(ctx)
    try:
        assert dep() == ctx
    finally:
        security_service.reset_internal_context(token_var)

    class ReqModel(BaseModel):
        tenant_id: str | None = None
        value: int

    token_var = security_service.set_internal_context(ctx)
    try:
        scoped = security_service.enforce_request_tenant(ReqModel(value=1))
        assert scoped.tenant_id == "tenant"
    finally:
        security_service.reset_internal_context(token_var)

    payload = {
        "iss": "watchdog-main",
        "aud": "resolver",
        "iat": 1_700_000_000,
        "exp": 4_700_000_000,
        "jti": "jti-3",
        "tenant_id": "tenant",
        "org_id": "org",
        "user_id": "u1",
        "username": "alice",
        "permissions": ["read"],
        "group_ids": ["g1"],
        "role": "user",
        "is_superuser": False,
    }
    headers = _context_headers(payload)
    auth_ctx = security_service.authenticate_internal_headers(headers)
    assert auth_ctx.tenant_id == "tenant"
    with pytest.raises(HTTPException):
        security_service.authenticate_internal_headers({"x-service-token": "wrong", "authorization": "Bearer x"})
    security_service.settings.expected_service_token = ""
    with pytest.raises(HTTPException):
        security_service.authenticate_internal_headers(headers)
    security_service.settings.expected_service_token = "internal-service-token"
    assert security_service._requires_internal_auth("/api/v1/query") is True
    assert security_service._requires_internal_auth("/api/v1/ready") is False
    assert security_service._requires_internal_auth("/health") is False
