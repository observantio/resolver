"""
Security service for validating internal requests, managing tenant context, and enforcing authentication policies.

Copyright (c) 2026 Stefan Kumarasinghe
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
"""

from __future__ import annotations

from contextvars import ContextVar, Token
from dataclasses import dataclass
from hmac import compare_digest
import logging
import threading
import time
from typing import Any, Mapping, Optional

import jwt
from fastapi import HTTPException, Request, status
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response

from config import ALLOWED_CONTEXT_ALGORITHMS, settings

_context_var: ContextVar["InternalContext | None"] = ContextVar("becertain_internal_context", default=None)
log = logging.getLogger(__name__)
_jti_seen_lock = threading.Lock()
_jti_seen_cache: dict[str, float] = {}


@dataclass(frozen=True)
class InternalContext:
    tenant_id: str
    org_id: str
    user_id: str
    username: str
    permissions: list[str]
    group_ids: list[str]
    role: str
    is_superuser: bool


def _context_algorithms() -> list[str]:
    raw = settings.context_algorithms or "HS256"
    parsed = [str(v).strip().upper() for v in str(raw).split(",") if str(v).strip()]
    algorithms = parsed or ["HS256"]
    invalid = sorted(set(algorithms) - ALLOWED_CONTEXT_ALGORITHMS)
    if invalid:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=(
                "Unsupported context token algorithm configuration: "
                + ",".join(invalid)
            ),
        )
    return algorithms


def _assert_jti_not_replayed(jti: str) -> None:
    now = time.monotonic()
    ttl = int(getattr(settings, "context_replay_ttl_seconds", 180) or 180)
    with _jti_seen_lock:
        stale = [token_id for token_id, ts in _jti_seen_cache.items() if now - ts > ttl]
        for token_id in stale:
            _jti_seen_cache.pop(token_id, None)
        if jti in _jti_seen_cache:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Replayed context token")
        _jti_seen_cache[jti] = now


def _parse_bearer(auth_header: str | None) -> str:
    if not auth_header:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing authorization header")
    parts = auth_header.split(" ", 1)
    if len(parts) != 2 or parts[0].lower() != "bearer" or not parts[1].strip():
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid authorization header")
    return parts[1].strip()


def _decode_context_token(token: str) -> dict[str, Any]:
    key = settings.context_verify_key
    if not key:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Missing context verify key")
    try:
        payload = jwt.decode(
            token,
            key,
            algorithms=_context_algorithms(),
            audience=settings.context_audience,
            issuer=settings.context_issuer,
            options={"require": ["exp", "iat", "iss", "aud", "jti"]},
        )
    except jwt.ExpiredSignatureError as exc:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Context token expired") from exc
    except jwt.InvalidTokenError as exc:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid context token") from exc
    try:
        iat = int(payload.get("iat"))
        exp = int(payload.get("exp"))
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid context token claims") from exc
    if exp <= iat:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid context token lifetime")
    jti = str(payload.get("jti") or "").strip()
    if not jti:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing context token jti")
    _assert_jti_not_replayed(jti)
    return payload


def _build_context(payload: dict[str, Any]) -> InternalContext:
    tenant_id = str(payload.get("tenant_id", "")).strip()
    if not tenant_id:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing tenant context")
    return InternalContext(
        tenant_id=tenant_id,
        org_id=str(payload.get("org_id", tenant_id)),
        user_id=str(payload.get("user_id", "")),
        username=str(payload.get("username", "")),
        permissions=list(payload.get("permissions") or []),
        group_ids=list(payload.get("group_ids") or []),
        role=str(payload.get("role", "user")),
        is_superuser=bool(payload.get("is_superuser", False)),
    )


def set_internal_context(ctx: InternalContext) -> Token:
    return _context_var.set(ctx)


def reset_internal_context(token: Token) -> None:
    _context_var.reset(token)


def get_internal_context() -> InternalContext | None:
    return _context_var.get()


def get_context_tenant(default_tenant: Optional[str] = None) -> str:
    ctx = get_internal_context()
    if ctx:
        return ctx.tenant_id
    if default_tenant:
        return default_tenant
    raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing tenant context")


def ensure_permission(permission: str) -> InternalContext:
    ctx = get_internal_context()
    if ctx is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing internal context")
    if ctx.is_superuser:
        return ctx
    if permission not in (ctx.permissions or []):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=f"Missing permission: {permission}")
    return ctx


def require_permission_dependency(permission: str):
    def _dependency() -> InternalContext:
        return ensure_permission(permission)

    return _dependency


def enforce_request_tenant(model: Any) -> Any:
    if model is None:
        return model
    tenant = get_context_tenant(getattr(model, "tenant_id", None))
    if hasattr(model, "model_copy"):
        return model.model_copy(update={"tenant_id": tenant})
    if hasattr(model, "copy"):
        return model.copy(update={"tenant_id": tenant})
    try:
        model.tenant_id = tenant
    except (AttributeError, TypeError) as exc:
        log.warning("Failed to apply tenant context to request model %s: %s", type(model).__name__, exc)
    return model


def authenticate_internal_request(request: Request) -> InternalContext:
    return authenticate_internal_headers(request.headers)


def authenticate_internal_headers(headers: Mapping[str, str]) -> InternalContext:
    expected_service_token = settings.expected_service_token
    if not expected_service_token:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Missing expected service token")

    provided_service_token = headers.get("x-service-token", "")
    if not compare_digest(provided_service_token, expected_service_token):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid service token")

    bearer = _parse_bearer(headers.get("authorization"))
    payload = _decode_context_token(bearer)
    return _build_context(payload)


def _requires_internal_auth(path: str) -> bool:
    return path.startswith("/api/v1") and path != "/api/v1/ready"


class InternalAuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next) -> Response:
        path = str(request.url.path or "")
        if not _requires_internal_auth(path):
            return await call_next(request)

        try:
            ctx = authenticate_internal_headers(request.headers)
        except HTTPException as exc:
            return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})

        token = set_internal_context(ctx)
        request.state.internal_context = ctx
        try:
            return await call_next(request)
        finally:
            reset_internal_context(token)
