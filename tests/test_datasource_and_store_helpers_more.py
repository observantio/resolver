"""
Copyright (c) 2026 Stefan Kumarasinghe.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. See http://www.apache.org/licenses/LICENSE-2.0 for details.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from connectors.common import BackendErrorMessages, query_backend_json
from datasources.data_config import DataSourceSettings
from datasources.factory import DataSourceFactory
from datasources.retry import retry
from store import baseline as baseline_store


@pytest.mark.asyncio
async def test_query_backend_json_and_store_baseline_helpers(monkeypatch):
    captured = {}

    async def fake_fetch_json(url, options=None, messages=None):
        options = options or SimpleNamespace(params=None, headers=None, timeout=30, client=None)
        messages = messages or SimpleNamespace(invalid_msg="", timeout_msg="", unavailable_msg="")
        captured.update(
            {
                "url": url,
                "params": options.params,
                "headers": options.headers,
                "timeout": options.timeout,
                "client": options.client,
                "invalid_msg": messages.invalid_msg,
                "timeout_msg": messages.timeout_msg,
                "unavailable_msg": messages.unavailable_msg,
            }
        )
        return {"ok": True}

    monkeypatch.setattr("connectors.common.fetch_json", fake_fetch_json)
    connector = type(
        "C",
        (),
        {
            "base_url": "https://backend",
            "timeout": 12,
            "client": object(),
            "_headers": lambda self: {"X-Scope-OrgID": "tenant"},
        },
    )()
    assert await query_backend_json(
        connector,
        path="/query",
        params={"q": "up"},
        messages=BackendErrorMessages(invalid="bad", timeout="slow", unavailable="down"),
    ) == {"ok": True}
    assert captured["url"] == "https://backend/query"
    assert captured["headers"] == {"X-Scope-OrgID": "tenant"}

    connector_with_request_headers = type(
        "C2",
        (),
        {
            "base_url": "https://backend",
            "timeout": 12,
            "client": object(),
            "request_headers": lambda self: {"X-From": "request_headers"},
        },
    )()
    assert await query_backend_json(
        connector_with_request_headers,
        path="/query2",
        params={"q": "up"},
        messages=BackendErrorMessages(invalid="bad", timeout="slow", unavailable="down"),
    ) == {"ok": True}
    assert captured["url"] == "https://backend/query2"
    assert captured["headers"] == {"X-From": "request_headers"}

    connector_with_non_callable_request_headers = type(
        "C3",
        (),
        {
            "base_url": "https://backend",
            "timeout": 12,
            "client": object(),
            "request_headers": {},
            "_headers": lambda self: {"X-Fallback": "compat"},
        },
    )()
    assert await query_backend_json(
        connector_with_non_callable_request_headers,
        "/query3",
        {"q": "up"},
        messages=BackendErrorMessages(invalid="i", timeout="t", unavailable="u"),
    ) == {"ok": True}
    assert captured["url"] == "https://backend/query3"
    assert captured["headers"] == {"X-Fallback": "compat"}

    baseline = baseline_store.Baseline(mean=1.0, std=2.0, lower=-5.0, upper=7.0, seasonal_mean=3.0, sample_count=4)
    raw = baseline_store._to_json(baseline)
    restored = baseline_store._from_json(raw)
    assert restored.mean == 1.0 and restored.sample_count == 4

    blended = baseline_store._blend(
        baseline_store.Baseline(mean=10.0, std=4.0, lower=0.0, upper=20.0, seasonal_mean=2.0, sample_count=20),
        baseline_store.Baseline(mean=20.0, std=2.0, lower=0.0, upper=30.0, seasonal_mean=5.0, sample_count=5),
    )
    assert blended.sample_count == 25
    assert blended.seasonal_mean == 5.0

    monkeypatch.setattr(baseline_store, "redis_get", lambda key: asyncio.sleep(0, result=raw))
    loaded = await baseline_store.load("tenant", "metric")
    assert loaded and loaded.upper == 7.0

    monkeypatch.setattr(baseline_store, "redis_get", lambda key: asyncio.sleep(0, result="{bad"))
    assert await baseline_store.load("tenant", "metric") is None
    monkeypatch.setattr(baseline_store, "redis_get", lambda key: (_ for _ in ()).throw(TypeError("bad")))
    assert await baseline_store.load("tenant", "metric") is None

    saved = []

    async def fake_redis_set(key, value, ttl=None):
        saved.append((key, value, ttl))

    monkeypatch.setattr(baseline_store, "redis_set", fake_redis_set)
    await baseline_store.save("tenant", "metric", baseline)
    assert saved and saved[0][2] == baseline_store.BASELINE_TTL

    async def failing_redis_set(key, value, ttl=None):
        raise ValueError("bad")

    monkeypatch.setattr(baseline_store, "redis_set", failing_redis_set)
    await baseline_store.save("tenant", "metric", baseline)

    fresh = baseline_store.Baseline(mean=1.0, std=1.0, lower=-2.0, upper=4.0, seasonal_mean=None, sample_count=2)
    cached = baseline_store.Baseline(mean=10.0, std=3.0, lower=1.0, upper=19.0, seasonal_mean=2.0, sample_count=25)
    monkeypatch.setattr(baseline_store, "compute", lambda ts, vals, z_threshold=3.0: fresh)
    monkeypatch.setattr(baseline_store, "load", lambda tenant_id, metric_name: asyncio.sleep(0, result=cached))
    persisted = []

    async def fake_save(tenant_id, metric_name, result):
        persisted.append(result)

    monkeypatch.setattr(baseline_store, "save", fake_save)
    result = await baseline_store.compute_and_persist("tenant", "metric", [1.0], [2.0])
    assert result.sample_count == 27
    assert persisted and persisted[0].sample_count == 27

    monkeypatch.setattr(baseline_store, "load", lambda tenant_id, metric_name: asyncio.sleep(0, result=None))
    result = await baseline_store.compute_and_persist("tenant", "metric", [1.0], [2.0])
    assert result is fresh


def test_datasource_settings_factory_and_retry(monkeypatch):
    settings = DataSourceSettings(
        logs_backend="loki",
        metrics_backend="mimir",
        traces_backend="tempo",
        loki_url="https://loki/",
        mimir_url="https://mimir/",
        tempo_url="https://tempo/",
    )
    assert settings.loki_url == "https://loki"
    assert settings.mimir_url == "https://mimir"
    assert settings.tempo_url == "https://tempo"

    with pytest.raises(ValueError):
        DataSourceSettings(logs_backend="bad")
    with pytest.raises(ValueError):
        DataSourceSettings(metrics_backend="bad")
    with pytest.raises(ValueError):
        DataSourceSettings(traces_backend="bad")

    monkeypatch.setattr(
        "datasources.factory.LokiConnector", lambda url, tenant_id, timeout=None: ("loki", url, tenant_id, timeout)
    )
    monkeypatch.setattr(
        "datasources.factory.MimirConnector", lambda url, tenant_id, timeout=None: ("mimir", url, tenant_id, timeout)
    )
    monkeypatch.setattr(
        "datasources.factory.TempoConnector", lambda url, tenant_id, timeout=None: ("tempo", url, tenant_id, timeout)
    )

    cfg = type(
        "Cfg",
        (),
        {
            "logs_backend": "loki",
            "metrics_backend": "mimir",
            "traces_backend": "tempo",
            "loki_url": "https://loki",
            "mimir_url": "https://mimir",
            "tempo_url": "https://tempo",
            "connector_timeout": 5,
        },
    )()
    assert DataSourceFactory.create_logs(cfg, "tenant")[0] == "loki"
    assert DataSourceFactory.create_metrics(cfg, "tenant")[0] == "mimir"
    assert DataSourceFactory.create_traces(cfg, "tenant")[0] == "tempo"

    cfg.metrics_backend = "bad"
    with pytest.raises(ValueError):
        DataSourceFactory.create_metrics(cfg, "tenant")
    cfg.logs_backend = "bad"
    with pytest.raises(ValueError):
        DataSourceFactory.create_logs(cfg, "tenant")
    cfg.traces_backend = "bad"
    with pytest.raises(ValueError):
        DataSourceFactory.create_traces(cfg, "tenant")

    sync_calls = []

    @retry(attempts=3, delay=1.0, backoff=2.0, exceptions=(ValueError,))
    def flaky_sync():
        sync_calls.append("x")
        if len(sync_calls) < 3:
            raise ValueError("retry")
        return "ok"

    sleep_calls = []
    monkeypatch.setattr("datasources.retry.time.sleep", lambda seconds: sleep_calls.append(seconds))
    assert flaky_sync() == "ok"
    assert sleep_calls == [1.0, 2.0]

    async_calls = []

    @retry(attempts=2, delay=0.5, backoff=3.0, exceptions=(ValueError,))
    async def flaky_async():
        async_calls.append("x")
        if len(async_calls) == 1:
            raise ValueError("retry")
        return "ok"

    async_sleep_calls = []

    async def fake_asyncio_sleep(seconds):
        async_sleep_calls.append(seconds)

    monkeypatch.setattr("datasources.retry.asyncio.sleep", fake_asyncio_sleep)
    assert asyncio.run(flaky_async()) == "ok"
    assert async_sleep_calls == [0.5]


def test_base_connector_request_headers_passthrough() -> None:
    from datasources.base import BaseConnector

    class _DummyConnector(BaseConnector):
        pass

    connector = _DummyConnector("tenant-a", "https://example", headers={"A": "b"})
    assert connector.request_headers() == {"A": "b", "X-Scope-OrgID": "tenant-a"}
    asyncio.run(connector.aclose())
