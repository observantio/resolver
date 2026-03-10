from __future__ import annotations

import pytest
from fastapi import HTTPException

from api.routes.exception import handle_exceptions


@pytest.mark.asyncio
async def test_handle_exceptions_wraps_async_success_and_errors():
    @handle_exceptions
    async def ok() -> str:
        return "ok"

    @handle_exceptions
    async def http_fail() -> str:
        raise HTTPException(status_code=409, detail="conflict")

    @handle_exceptions
    async def fail() -> str:
        raise RuntimeError("async boom")

    assert await ok() == "ok"
    with pytest.raises(HTTPException) as http_exc:
        await http_fail()
    assert http_exc.value.status_code == 409
    with pytest.raises(HTTPException) as exc:
        await fail()
    assert exc.value.status_code == 500
    assert exc.value.detail == "async boom"


def test_handle_exceptions_wraps_sync_success_and_errors():
    @handle_exceptions
    def ok() -> str:
        return "ok"

    @handle_exceptions
    def http_fail() -> str:
        raise HTTPException(status_code=404, detail="missing")

    @handle_exceptions
    def fail() -> str:
        raise ValueError("sync boom")

    assert ok() == "ok"
    with pytest.raises(HTTPException) as http_exc:
        http_fail()
    assert http_exc.value.status_code == 404
    with pytest.raises(HTTPException) as exc:
        fail()
    assert exc.value.status_code == 500
    assert exc.value.detail == "sync boom"