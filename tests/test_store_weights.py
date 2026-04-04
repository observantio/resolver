"""
Test Suite for Store Weights.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
"""

import pytest

from store import weights as wstore


@pytest.mark.asyncio
async def test_weights_lifecycle():
    tid = "tenant123"
    assert await wstore.load(tid) is None
    data = {"metrics": 0.6, "logs": 0.4}
    await wstore.save(tid, data, update_count=5)
    stored = await wstore.load(tid)
    assert stored["weights"] == data
    assert stored["update_count"] == 5
    await wstore.delete(tid)
    assert await wstore.load(tid) is None


def test_weights_key_format():
    tid = "foo"
    from store import keys

    k = keys.weights(tid)
    assert "foo" in k
    assert k.startswith("bc:foo:weights")


@pytest.mark.asyncio
async def test_weights_load_rejects_invalid_payload(monkeypatch):
    async def fake_redis_get(_):
        return '{"weights":"not-a-map","update_count":"oops"}'

    monkeypatch.setattr(wstore, "redis_get", fake_redis_get)
    assert await wstore.load("tenant-bad") is None


@pytest.mark.asyncio
async def test_weights_load_coerces_update_count(monkeypatch):
    async def fake_redis_get(_):
        return '{"weights":{"metrics":0.4},"update_count":"7"}'

    monkeypatch.setattr(wstore, "redis_get", fake_redis_get)
    loaded = await wstore.load("tenant-good")
    assert loaded is not None
    assert loaded["weights"] == {"metrics": 0.4}
    assert loaded["update_count"] == 7
