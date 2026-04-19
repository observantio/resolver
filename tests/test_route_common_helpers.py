"""
Copyright (c) 2026 Stefan Kumarasinghe.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. See http://www.apache.org/licenses/LICENSE-2.0 for details.
"""

from __future__ import annotations

import types

import pytest
from fastapi import HTTPException

from api.routes import common


class _DummyProvider:
    def __init__(self, tenant_id: str, settings: object):
        self.tenant_id = tenant_id
        self.settings = settings
        self.closed = False

    async def aclose(self) -> None:
        self.closed = True


class _DefaultValue:
    def __init__(self, default):
        self.default = default


@pytest.mark.asyncio
async def test_get_provider_caches_resolved_tenant(monkeypatch):
    common._providers.clear()
    monkeypatch.setattr(common, "get_context_tenant", lambda tenant_id=None: "tenant-from-context")
    monkeypatch.setattr(common, "DataSourceProvider", _DummyProvider)

    first = common.get_provider("tenant-in-request")
    second = common.get_provider("different")

    assert first is second
    assert first.tenant_id == "tenant-from-context"
    assert set(common._providers) == {"tenant-from-context"}


@pytest.mark.asyncio
async def test_close_providers_closes_everything(monkeypatch):
    first = _DummyProvider("t1", object())
    second = _DummyProvider("t2", object())
    common._providers.clear()
    common._providers.update({"t1": first, "t2": second})

    await common.close_providers()

    assert first.closed is True
    assert second.closed is True
    assert common._providers == {}


@pytest.mark.asyncio
async def test_safe_call_success_and_failure():
    async def good() -> str:
        return "ok"

    async def bad() -> str:
        raise RuntimeError("boom")

    assert await common.safe_call(good()) == "ok"
    with pytest.raises(HTTPException) as exc:
        await common.safe_call(bad(), status_code=418)
    assert exc.value.status_code == 418
    assert exc.value.detail == "boom"


@pytest.mark.asyncio
async def test_value_helpers_and_fetch_requested_metrics(monkeypatch):
    captured = {}

    async def fake_fetch_metrics(provider, queries, start, end, step):
        captured.update(
            {
                "provider": provider,
                "queries": queries,
                "start": start,
                "end": end,
                "step": step,
            }
        )
        return [("cpu", {"result": []})]

    monkeypatch.setattr(common, "DEFAULT_METRIC_QUERIES", ["builtin_cpu", "builtin_mem"])
    monkeypatch.setattr(common, "fetch_metrics", fake_fetch_metrics)
    provider = object()
    req = types.SimpleNamespace(metric_queries=["custom_q", "builtin_cpu", "custom_q"], start=10, end=20, step="30s")

    assert common.to_nanoseconds(7) == 7_000_000_000
    assert common.coerce_query_value(_DefaultValue("9"), int) == 9
    assert common.coerce_query_value("11", int) == 11
    result = await common.fetch_requested_metrics(provider, req)

    assert result == [("cpu", {"result": []})]
    assert captured["provider"] is provider
    assert captured["queries"] == ["custom_q", "builtin_cpu", "builtin_mem"]
    assert captured["start"] == 10
    assert captured["end"] == 20
    assert captured["step"] == "30s"
