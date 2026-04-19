"""
Copyright (c) 2026 Stefan Kumarasinghe.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. See http://www.apache.org/licenses/LICENSE-2.0 for details.
"""

from __future__ import annotations

import inspect

import pytest
from fastapi import HTTPException

from api.routes.exception import handle_exceptions


@pytest.mark.asyncio
async def test_handle_exceptions_wraps_async_success_and_errors():
    conflict = HTTPException(status_code=409, detail="conflict")

    @handle_exceptions
    async def ok() -> str:
        return "ok"

    @handle_exceptions
    async def http_fail() -> str:
        raise conflict

    @handle_exceptions
    async def fail() -> str:
        raise RuntimeError("async boom")

    assert await ok() == "ok"
    with pytest.raises(HTTPException) as http_exc:
        await http_fail()
    assert http_exc.value is conflict
    assert http_exc.value.status_code == 409
    with pytest.raises(HTTPException) as exc:
        await fail()
    assert exc.value.status_code == 500
    assert exc.value.detail == "async boom"
    assert isinstance(exc.value.__cause__, RuntimeError)


def test_handle_exceptions_wraps_sync_success_and_errors():
    missing = HTTPException(status_code=404, detail="missing")

    @handle_exceptions
    def ok() -> str:
        return "ok"

    @handle_exceptions
    def http_fail() -> str:
        raise missing

    @handle_exceptions
    def fail() -> str:
        raise ValueError("sync boom")

    assert ok() == "ok"
    with pytest.raises(HTTPException) as http_exc:
        http_fail()
    assert http_exc.value is missing
    assert http_exc.value.status_code == 404
    with pytest.raises(HTTPException) as exc:
        fail()
    assert exc.value.status_code == 500
    assert exc.value.detail == "sync boom"
    assert isinstance(exc.value.__cause__, ValueError)


def test_handle_exceptions_preserves_sync_async_shapes():
    @handle_exceptions
    async def async_handler() -> str:
        return "ok"

    @handle_exceptions
    def sync_handler() -> str:
        return "ok"

    assert inspect.iscoroutinefunction(async_handler)
    assert not inspect.iscoroutinefunction(sync_handler)
