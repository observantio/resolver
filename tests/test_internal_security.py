"""
Test Internal Security logic for the analysis engine, including authentication and authorization of internal service
requests, context management, and enforcement of tenant scope based on JWT tokens.

Copyright (c) 2026 Stefan Kumarasinghe.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. See http://www.apache.org/licenses/LICENSE-2.0 for details.
"""

import asyncio
import json
import uuid

import jwt
import pytest
from pydantic import BaseModel
from starlette.responses import JSONResponse

import services.security_service as security_service
from config import settings
from services.security_service import (
    InternalAuthMiddleware,
    InternalContext,
    enforce_request_tenant,
    get_context_tenant,
    reset_internal_context,
    set_internal_context,
)


def _headers(payload):
    token = jwt.encode(payload, settings.context_verify_key, algorithm="HS256")
    return {
        "x-service-token": settings.expected_service_token,
        "authorization": f"Bearer {token}",
    }


def _set_security_defaults():
    settings.expected_service_token = "internal-service-token"
    settings.context_verify_key = "very-secret-signing-key-with-32-bytes"
    settings.context_issuer = "watchdog-main"
    settings.context_audience = "resolver"
    settings.context_algorithms = "HS256"
    settings.context_replay_ttl_seconds = 180


@pytest.fixture(autouse=True)
def _clear_replay_cache():
    with security_service._jti_seen_lock:
        security_service._jti_seen_cache.clear()


async def _run_request(path: str, headers: dict[str, str]):
    async def app(scope, receive, send):
        payload = {"tenant_id": get_context_tenant("spoofed-tenant")}
        response = JSONResponse(payload)
        await response(scope, receive, send)

    middleware = InternalAuthMiddleware(app)

    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "http",
        "path": path,
        "raw_path": path.encode("ascii"),
        "query_string": b"",
        "headers": [(k.encode("latin1"), v.encode("latin1")) for k, v in headers.items()],
        "client": ("127.0.0.1", 12345),
        "server": ("testserver", 80),
    }

    messages: list[dict] = []

    async def receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message):
        messages.append(message)

    await middleware(scope, receive, send)

    status = next(m["status"] for m in messages if m["type"] == "http.response.start")
    body = b"".join(m.get("body", b"") for m in messages if m["type"] == "http.response.body")
    data = json.loads(body.decode("utf-8")) if body else {}
    return status, data


def test_missing_service_token_rejected():
    _set_security_defaults()
    status, _ = asyncio.run(_run_request("/api/v1/tenant", headers={}))
    assert status == 401


def test_invalid_context_token_rejected():
    _set_security_defaults()
    status, _ = asyncio.run(
        _run_request(
            "/api/v1/tenant",
            headers={"x-service-token": settings.expected_service_token, "authorization": "Bearer invalid"},
        )
    )
    assert status == 401


def test_valid_context_enforces_tenant_scope():
    _set_security_defaults()
    headers = _headers(
        {
            "iss": settings.context_issuer,
            "aud": settings.context_audience,
            "iat": 1_700_000_000,
            "exp": 4_700_000_000,
            "jti": str(uuid.uuid4()),
            "tenant_id": "tenant-from-context",
            "org_id": "tenant-from-context",
            "user_id": "u1",
            "username": "alice",
        }
    )
    status, payload = asyncio.run(_run_request("/api/v1/tenant", headers=headers))
    assert status == 200
    assert payload["tenant_id"] == "tenant-from-context"


def test_enforce_request_tenant_overrides_payload():
    token = set_internal_context(
        InternalContext(
            tenant_id="ctx-tenant",
            org_id="ctx-tenant",
            user_id="u1",
            username="alice",
            permissions=[],
            group_ids=[],
            role="user",
            is_superuser=False,
        )
    )

    class Req(BaseModel):
        tenant_id: str
        start: int
        end: int

    try:
        req = Req(tenant_id="spoofed", start=1, end=2)
        scoped = enforce_request_tenant(req)
        assert scoped.tenant_id == "ctx-tenant"
    finally:
        reset_internal_context(token)


def test_context_token_missing_jti_rejected():
    _set_security_defaults()
    headers = _headers(
        {
            "iss": settings.context_issuer,
            "aud": settings.context_audience,
            "iat": 1_700_000_000,
            "exp": 4_700_000_000,
            "jti": " ",
            "tenant_id": "tenant-from-context",
            "org_id": "tenant-from-context",
            "user_id": "u1",
            "username": "alice",
        }
    )
    status, payload = asyncio.run(_run_request("/api/v1/tenant", headers=headers))
    assert status == 401
    assert payload["detail"] == "Missing context token jti"


def test_context_token_replay_rejected():
    _set_security_defaults()
    jti = str(uuid.uuid4())
    headers = _headers(
        {
            "iss": settings.context_issuer,
            "aud": settings.context_audience,
            "iat": 1_700_000_000,
            "exp": 4_700_000_000,
            "jti": jti,
            "tenant_id": "tenant-from-context",
            "org_id": "tenant-from-context",
            "user_id": "u1",
            "username": "alice",
        }
    )
    status_1, _ = asyncio.run(_run_request("/api/v1/tenant", headers=headers))
    status_2, payload_2 = asyncio.run(_run_request("/api/v1/tenant", headers=headers))
    assert status_1 == 200
    assert status_2 == 401
    assert payload_2["detail"] == "Replayed context token"
