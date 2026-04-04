"""
Test suite for validating that route permissions are correctly wired and enforced in the API service.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
"""

from __future__ import annotations

import pytest
from fastapi import HTTPException
from fastapi.routing import APIRoute

from api.routes import analyze as analyze_route
from api.routes import events as events_route
from api.routes import ml as ml_route
from services.security_service import (
    InternalContext,
    ensure_permission,
    get_internal_context,
    reset_internal_context,
    set_internal_context,
)


def _context(permissions: list[str]) -> InternalContext:
    return InternalContext(
        tenant_id="tenant-ctx",
        org_id="tenant-ctx",
        user_id="u1",
        username="alice",
        permissions=permissions,
        group_ids=[],
        role="user",
        is_superuser=False,
    )


def test_ensure_permission_requires_context():
    assert get_internal_context() is None
    with pytest.raises(HTTPException) as exc_info:
        ensure_permission("read:rca")
    assert exc_info.value.status_code == 401


def test_ensure_permission_denies_missing_permission():
    token = set_internal_context(_context(["read:rca"]))
    try:
        with pytest.raises(HTTPException) as exc_info:
            ensure_permission("delete:rca")
    finally:
        reset_internal_context(token)
    assert exc_info.value.status_code == 403


def test_ensure_permission_allows_valid_permission():
    token = set_internal_context(_context(["create:rca"]))
    try:
        ctx = ensure_permission("create:rca")
    finally:
        reset_internal_context(token)
    assert ctx.user_id == "u1"


def _route_permission(route: APIRoute) -> str | None:
    for dependency in route.dependencies:
        fn = dependency.dependency
        closure = getattr(fn, "__closure__", None) if fn is not None else None
        if closure is None:
            continue
        for cell in closure:
            value = cell.cell_contents
            if isinstance(value, str) and value.endswith(":rca"):
                return value
    return None


def test_non_job_routes_wire_expected_permissions():
    observed = {}
    for module_router in (analyze_route.router, events_route.router, ml_route.router):
        for route in module_router.routes:
            if not isinstance(route, APIRoute):
                continue
            key = (sorted(route.methods)[0], route.path)
            observed[key] = _route_permission(route)

    assert observed[("POST", "/analyze")] == "create:rca"
    assert observed[("POST", "/events/deployment")] == "create:rca"
    assert observed[("GET", "/events/deployments")] == "read:rca"
    assert observed[("DELETE", "/events/deployments")] == "delete:rca"
    assert observed[("POST", "/ml/weights/feedback")] == "create:rca"
    assert observed[("GET", "/ml/weights")] == "read:rca"
    assert observed[("POST", "/ml/weights/reset")] == "delete:rca"
