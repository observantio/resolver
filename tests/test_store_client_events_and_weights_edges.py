"""
Copyright (c) 2026 Stefan Kumarasinghe.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
"""

from __future__ import annotations

import importlib
import json
from types import SimpleNamespace

import pytest

import store.client as store_client_module
from engine.causal.granger import GrangerResult
from engine.events.models import DeploymentEvent
from store import events as events_store
from store import granger as granger_store
from store import weights as weights_store


class _FakePipeline:
    def __init__(self, *, error: Exception | None = None):
        self.calls: list[tuple] = []
        self.error = error

    def rpush(self, key: str, value: str) -> object:
        self.calls.append(("rpush", key, value))
        return None

    def ltrim(self, key: str, start: int, end: int) -> object:
        self.calls.append(("ltrim", key, start, end))
        return None

    def expire(self, key: str, ttl: int) -> object:
        self.calls.append(("expire", key, ttl))
        return None

    async def execute(self) -> object:
        if self.error is not None:
            raise self.error
        self.calls.append(("execute",))
        return None


class _FakeRedisClient:
    def __init__(self, *, error: Exception | None = None):
        self.error = error
        self.values: dict[str, str] = {}
        self.lists: dict[str, list[str]] = {}
        self.pipeline_obj = _FakePipeline(error=error)

    async def ping(self) -> object:
        if self.error is not None:
            raise self.error
        return "PONG"

    async def get(self, key: str):
        if self.error is not None:
            raise self.error
        return self.values.get(key)

    async def setex(self, key: str, ttl: int, value: str) -> object:
        if self.error is not None:
            raise self.error
        self.values[key] = value
        return ttl

    async def set(self, key: str, value: str) -> object:
        if self.error is not None:
            raise self.error
        self.values[key] = value
        return True

    async def delete(self, key: str) -> object:
        if self.error is not None:
            raise self.error
        self.values.pop(key, None)
        self.lists.pop(key, None)
        return 1

    def pipeline(self):
        return self.pipeline_obj

    async def lrange(self, key: str, start: int, end: int) -> list[str]:
        if self.error is not None:
            raise self.error
        return list(self.lists.get(key, []))

    async def scan_iter(self, pattern: str):
        if self.error is not None:
            raise self.error
        for key in sorted(self.values):
            yield key


def _fresh_store_client():
    return importlib.reload(store_client_module)


@pytest.mark.asyncio
async def test_store_client_get_redis_success_and_cooldown(monkeypatch):
    client_mod = _fresh_store_client()
    client_mod._fallback.clear()
    client_mod._fallback_lists.clear()
    client_mod._REDIS_CLIENT = None
    client_mod._RETRY_AFTER_MONOTONIC = 0.0
    client_mod._USING_FALLBACK = False

    fake_client = _FakeRedisClient()
    import_calls: list[str] = []

    def fake_import(name: str):
        import_calls.append(name)
        if name == "redis.asyncio":
            return SimpleNamespace(from_url=lambda *args, **kwargs: fake_client)
        raise ImportError(name)

    monkeypatch.setattr(client_mod, "import_module", fake_import)
    first = await client_mod.get_redis()
    second = await client_mod.get_redis()
    assert first is fake_client
    assert second is fake_client
    assert import_calls == ["redis.asyncio"]
    assert client_mod.is_using_fallback() is False

    client_mod = _fresh_store_client()
    client_mod._fallback.clear()
    client_mod._fallback_lists.clear()
    client_mod._REDIS_CLIENT = None
    client_mod._RETRY_AFTER_MONOTONIC = 0.0
    client_mod._USING_FALLBACK = False
    attempts = {"count": 0}
    clock = {"calls": 0}

    def fail_import(name: str):
        attempts["count"] += 1
        raise ImportError(name)

    def fake_monotonic() -> float:
        clock["calls"] += 1
        return 10.0 if clock["calls"] <= 2 else 12.0

    monkeypatch.setattr(client_mod, "import_module", fail_import)
    monkeypatch.setattr(client_mod.time, "monotonic", fake_monotonic)
    assert await client_mod.get_redis() is None
    assert await client_mod.get_redis() is None
    assert attempts["count"] == 1
    assert client_mod.is_using_fallback() is True


@pytest.mark.asyncio
async def test_store_client_ops_success_and_error_fallback(monkeypatch):
    client_mod = _fresh_store_client()
    client_mod._fallback.clear()
    client_mod._fallback_lists.clear()
    working_client = _FakeRedisClient()
    working_client.lists["events"] = ["a"]
    monkeypatch.setattr(client_mod, "get_redis", lambda: _resolved(working_client))

    await client_mod.redis_set("key", "value")
    await client_mod.redis_set("ttl-key", "ttl-value", ttl=30)
    assert await client_mod.redis_get("key") == "value"
    await client_mod.redis_rpush("events", "b", ttl=10, max_len=2)
    assert ("rpush", "events", "b") in working_client.pipeline_obj.calls
    assert ("ltrim", "events", -2, -1) in working_client.pipeline_obj.calls
    assert ("expire", "events", 10) in working_client.pipeline_obj.calls
    assert await client_mod.redis_lrange("events") == ["a"]
    assert await client_mod.redis_scan("*") == ["key", "ttl-key"]
    await client_mod.redis_delete("key")
    assert "key" not in working_client.values

    failing_client = _FakeRedisClient(error=OSError("down"))
    monkeypatch.setattr(client_mod, "get_redis", lambda: _resolved(failing_client))
    client_mod._fallback["cached"] = "fallback-value"
    client_mod._fallback_lists["events"] = ["old"]
    await client_mod.redis_set("new", "value")
    assert client_mod._fallback["new"] == "value"
    assert await client_mod.redis_get("cached") == "fallback-value"
    await client_mod.redis_rpush("events", "new-item", max_len=2)
    assert client_mod._fallback_lists["events"] == ["old", "new-item"]
    assert await client_mod.redis_lrange("events") == ["old", "new-item"]
    assert sorted(await client_mod.redis_scan("*")) == ["cached", "new"]
    await client_mod.redis_delete("cached")
    assert "cached" not in client_mod._fallback


async def _resolved(value):
    return value


@pytest.mark.asyncio
async def test_events_store_coercion_load_and_append(monkeypatch):
    assert events_store._coerce_float("1.25") == 1.25
    with pytest.raises(TypeError):
        events_store._coerce_float(True)
    assert events_store._coerce_event(
        {"service": "svc", "timestamp": "1", "version": "v1", "metadata": {"a": "b"}}
    ) == {
        "service": "svc",
        "timestamp": 1.0,
        "version": "v1",
        "author": "",
        "environment": "production",
        "source": "unknown",
        "metadata": {"a": "b"},
    }
    assert events_store._coerce_event({"service": "svc", "timestamp": True, "version": "v1"}) is None

    valid_event = json.dumps(
        {
            "service": "svc",
            "timestamp": 2.0,
            "version": "v2",
            "author": "me",
            "environment": "prod",
            "source": "ci",
            "metadata": {"sha": "abc"},
        }
    )
    monkeypatch.setattr(events_store, "redis_lrange", lambda key: _resolved([valid_event]))
    loaded = await events_store.load("tenant-a")
    assert loaded[0]["service"] == "svc"

    monkeypatch.setattr(events_store, "redis_lrange", lambda key: _resolved(["not-json"]))
    assert await events_store.load("tenant-a") == []

    recorded = []

    async def fake_rpush(key, value, ttl=None, max_len=None):
        recorded.append((key, json.loads(value), ttl, max_len))

    monkeypatch.setattr(events_store, "redis_rpush", fake_rpush)
    await events_store.append(
        "tenant-a", DeploymentEvent(service="svc", timestamp=1.0, version="v1", metadata={"a": "b"})
    )
    assert recorded[0][1]["service"] == "svc"
    assert recorded[0][3] == 500

    monkeypatch.setattr(events_store, "redis_rpush", lambda *args, **kwargs: _raise(TypeError("bad")))
    await events_store.append("tenant-a", DeploymentEvent(service="svc", timestamp=1.0, version="v1"))

    deleted = []
    monkeypatch.setattr(events_store, "redis_delete", lambda key: _resolved(deleted.append(key)))
    await events_store.clear("tenant-a")
    assert deleted


@pytest.mark.asyncio
async def test_weights_and_granger_edge_coercions(monkeypatch):
    monkeypatch.setattr(
        weights_store, "redis_get", lambda key: _resolved('{"weights":{"metrics":0.4},"update_count":true}')
    )
    loaded = await weights_store.load("tenant-a")
    assert loaded == {"weights": {"metrics": 0.4}, "update_count": 1}

    monkeypatch.setattr(
        weights_store, "redis_get", lambda key: _resolved('{"weights":{"metrics":0.4},"update_count":2.9}')
    )
    loaded = await weights_store.load("tenant-a")
    assert loaded == {"weights": {"metrics": 0.4}, "update_count": 2}

    monkeypatch.setattr(
        weights_store, "redis_get", lambda key: _resolved('{"weights":{"metrics":0.4},"update_count":"oops"}')
    )
    loaded = await weights_store.load("tenant-a")
    assert loaded == {"weights": {"metrics": 0.4}, "update_count": 0}

    monkeypatch.setattr(weights_store, "redis_set", lambda *args, **kwargs: _raise(TypeError("bad")))
    await weights_store.save("tenant-a", {"metrics": 0.3}, 1)

    assert granger_store._coerce_int(3.8) == 3
    assert granger_store._coerce_float("2.5") == 2.5
    with pytest.raises(TypeError):
        granger_store._coerce_int(True)
    assert granger_store._pair_key("a", "b") == "a>>>b"

    monkeypatch.setattr(granger_store, "redis_get", lambda key: _resolved('{"not":"a-list"}'))
    assert await granger_store.load("tenant-a", "svc") == []

    monkeypatch.setattr(
        granger_store,
        "redis_get",
        lambda key: _resolved(
            json.dumps(
                [
                    {
                        "cause_metric": "a",
                        "effect_metric": "b",
                        "max_lag": "2",
                        "f_statistic": "1.5",
                        "p_value": 0.1,
                        "is_causal": True,
                        "strength": 0.6,
                    },
                    {
                        "cause_metric": "x",
                        "effect_metric": "y",
                        "max_lag": True,
                        "f_statistic": 1.5,
                        "p_value": 0.1,
                        "is_causal": True,
                        "strength": 0.2,
                    },
                ]
            )
        ),
    )
    loaded = await granger_store.load("tenant-a", "svc")
    assert loaded == [
        {
            "cause_metric": "a",
            "effect_metric": "b",
            "max_lag": 2,
            "f_statistic": 1.5,
            "p_value": 0.1,
            "is_causal": True,
            "strength": 0.6,
        }
    ]

    monkeypatch.setattr(granger_store, "redis_set", lambda *args, **kwargs: _raise(TypeError("bad")))
    merged = await granger_store.save_and_merge(
        "tenant-a",
        "svc",
        [GrangerResult("a", "b", 1, 1.2, 0.1, True, 0.7), GrangerResult("a", "b", 1, 1.2, 0.1, True, 0.5)],
    )
    assert merged[0]["strength"] == 0.7

    async def fake_load(tenant_id: str, service: str):
        if service == "svc-a":
            return [
                {
                    "cause_metric": "a",
                    "effect_metric": "b",
                    "max_lag": 1,
                    "f_statistic": 1.0,
                    "p_value": 0.1,
                    "is_causal": True,
                    "strength": 0.3,
                }
            ]
        return [
            {
                "cause_metric": "a",
                "effect_metric": "b",
                "max_lag": 1,
                "f_statistic": 1.0,
                "p_value": 0.1,
                "is_causal": True,
                "strength": 0.8,
            }
        ]

    monkeypatch.setattr(granger_store, "load", fake_load)
    combined = await granger_store.load_all_services("tenant-a", ["svc-a", "svc-b"])
    assert combined[0]["strength"] == 0.8


async def _raise(exc: Exception):
    raise exc
