"""
Test Suite for Store Client.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
"""

import pytest

from store import client as store_client
from store.client import _fallback, redis_delete, redis_get, redis_scan, redis_set


@pytest.mark.asyncio
async def test_fallback_operations(monkeypatch):
    async def no_redis():
        return None

    monkeypatch.setattr(store_client, "get_redis", no_redis)
    _fallback.clear()
    await redis_set("k1", "v1")
    assert await redis_get("k1") == "v1"
    await redis_delete("k1")
    assert await redis_get("k1") is None


@pytest.mark.asyncio
async def test_keys_pattern(monkeypatch):
    async def no_redis():
        return None

    monkeypatch.setattr(store_client, "get_redis", no_redis)
    _fallback.clear()
    await redis_set("abc", "1")
    await redis_set("abx", "2")
    keys = await redis_scan("ab*")
    assert "abc" in keys and "abx" in keys
