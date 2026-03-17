"""
Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
"""

from __future__ import annotations

import asyncio
import importlib
import runpy
import sys
import types

import numpy as np
import pytest

from api.requests import ChangepointRequest, CorrelateRequest, LogRequest, MetricRequest
from api.requests import TraceRequest
from api.responses.base import NpModel, _coerce
from api.routes import correlation as correlation_route
from api.routes import events as events_route
from api.routes import forecast as forecast_route
from api.routes import logs as logs_route
from api.routes import metrics as metrics_route
from api.routes import traces as traces_route
from custom_types import json as json_types
import main as app_main


class DemoModel(NpModel):
    payload: object


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ({"i": np.int64(4), "f": np.float64(1.5), "a": np.array([1, 2])}, {"i": 4, "f": 1.5, "a": [1, 2]}),
        ([np.int64(2), np.float64(3.5), np.array([4, 5])], [2, 3.5, [4, 5]]),
    ],
)
def test_np_model_and_coerce_handle_numpy_values(value, expected):
    assert _coerce(value) == expected
    assert DemoModel(payload=value).model_dump()["payload"] == expected


@pytest.mark.asyncio
async def test_metric_trajectory_sorts_and_limits_results(monkeypatch):
    req = CorrelateRequest(tenant_id="tenant-a", start=10, end=20)
    provider = object()

    monkeypatch.setattr(forecast_route, "coerce_query_value", lambda value, typ: typ(value))
    monkeypatch.setattr(forecast_route, "enforce_request_tenant", lambda current: current)
    monkeypatch.setattr(forecast_route, "get_provider", lambda tenant_id: provider)

    async def fake_fetch_requested_metrics(current_provider, current_req):
        assert current_provider is provider
        assert current_req is req
        return [("cpu", "cpu-raw"), ("disk", "disk-raw")]

    def fake_iter_series(resp, query_hint=None):
        if resp == "cpu-raw":
            yield ("cpu_usage", [1, 2], [3.0, 4.0])
        else:
            yield ("disk_latency", [1, 2], [5.0, 6.0])

    monkeypatch.setattr(forecast_route, "fetch_requested_metrics", fake_fetch_requested_metrics)
    monkeypatch.setattr(forecast_route.anomaly, "iter_series", fake_iter_series)
    monkeypatch.setattr(forecast_route, "FORECAST_THRESHOLDS", {"cpu": 0.9})
    monkeypatch.setattr(
        forecast_route,
        "forecast",
        lambda metric_name, ts, vals, threshold: types.SimpleNamespace(severity="high", threshold=threshold)
        if metric_name == "cpu_usage"
        else None,
    )
    monkeypatch.setattr(
        forecast_route,
        "analyze_degradation",
        lambda metric_name, ts, vals: types.SimpleNamespace(severity="medium")
        if metric_name == "cpu_usage"
        else types.SimpleNamespace(severity="critical"),
    )

    result = await forecast_route.metric_trajectory(req, limit=1)

    assert result == {
        "results": [
            {
                "metric": "disk_latency",
                "forecast": None,
                "degradation": {"severity": "critical"},
            }
        ]
    }


def test_forecast_severity_value_handles_non_dict_payloads():
    assert forecast_route._severity_value("bad") == ""
    assert forecast_route._severity_value({"severity": 4}) == ""
    assert forecast_route._severity_value({"severity": "HIGH"}) == "high"


@pytest.mark.asyncio
async def test_event_routes_and_json_type_guards(monkeypatch):
    recorded = {}

    class DummyRegistry:
        async def register_event(self, tenant_id, event):
            recorded["registered"] = (tenant_id, event.service, event.version)

        async def get_events(self, tenant_id):
            recorded["listed"] = tenant_id
            return [{
                "service": "svc",
                "timestamp": 12,
                "version": "v1",
                "author": "me",
                "environment": "prod",
                "source": "api",
                "metadata": {"ok": True},
            }]

        async def clear_events(self, tenant_id):
            recorded["cleared"] = tenant_id

    monkeypatch.setattr(events_route, "enforce_request_tenant", lambda req: req)
    monkeypatch.setattr(events_route, "get_context_tenant", lambda tenant_id: tenant_id)
    monkeypatch.setattr(events_route, "get_registry", lambda: DummyRegistry())

    request = importlib.import_module("api.requests").DeploymentEventRequest(
        tenant_id="tenant-a", service="svc", timestamp=12, version="v1"
    )

    assert await events_route.register_deployment(request) == {
        "status": "registered",
        "service": "svc",
        "version": "v1",
    }
    assert await events_route.list_deployments("tenant-a") == [{
        "service": "svc",
        "timestamp": 12,
        "version": "v1",
        "author": "me",
        "environment": "prod",
        "source": "api",
        "metadata": {"ok": True},
    }]
    assert await events_route.clear_deployments("tenant-a") == {
        "status": "cleared",
        "tenant_id": "tenant-a",
    }
    assert recorded == {
        "registered": ("tenant-a", "svc", "v1"),
        "listed": "tenant-a",
        "cleared": "tenant-a",
    }

    assert json_types.is_json_value({"items": [1, 2.5, None, {"ok": True}]}) is True
    assert json_types.is_json_value({1: "bad-key"}) is False
    assert json_types.is_json_value(b"nope") is False
    assert json_types.is_json_object({"payload": ["x", 1]}) is True
    assert json_types.is_json_object(["not", "an", "object"]) is False


@pytest.mark.asyncio
async def test_register_deployment_rejects_blank_tenant(monkeypatch):
    monkeypatch.setattr(events_route, "enforce_request_tenant", lambda req: req)
    monkeypatch.setattr(events_route, "get_context_tenant", lambda tenant_id: "  ")

    request = importlib.import_module("api.requests").DeploymentEventRequest(
        tenant_id="tenant-a", service="svc", timestamp=12, version="v1"
    )

    with pytest.raises(Exception):
        await events_route.register_deployment(request)


@pytest.mark.asyncio
async def test_log_routes_call_provider_and_analysis(monkeypatch):
    calls = []

    class DummyProvider:
        async def query_logs(self, query, start, end):
            calls.append((query, start, end))
            return {"query": query, "start": start, "end": end}

    async def fake_safe_call(awaitable):
        return await awaitable

    monkeypatch.setattr(logs_route, "enforce_request_tenant", lambda req: req)
    monkeypatch.setattr(logs_route, "get_provider", lambda tenant_id: DummyProvider())
    monkeypatch.setattr(logs_route, "safe_call", fake_safe_call)
    monkeypatch.setattr(logs_route, "to_nanoseconds", lambda value: value * 10)
    monkeypatch.setattr(logs_route.logs, "analyze", lambda raw: [{"kind": "pattern", **raw}])
    monkeypatch.setattr(logs_route.logs, "detect_bursts", lambda raw: [{"kind": "burst", **raw}])

    req = LogRequest(tenant_id="tenant-a", query="{job='api'}", start=2, end=4)

    assert await logs_route.log_patterns(req) == [
        {"kind": "pattern", "query": "{job='api'}", "start": 20, "end": 40}
    ]
    assert await logs_route.log_bursts(req) == [
        {"kind": "burst", "query": "{job='api'}", "start": 20, "end": 40}
    ]
    assert calls == [
        ("{job='api'}", 20, 40),
        ("{job='api'}", 20, 40),
    ]


@pytest.mark.asyncio
async def test_metric_anomalies_and_changepoints_routes_cover_fallbacks(monkeypatch):
    class DummyProvider:
        async def query_metrics(self, query, start, end, step):
            return {"query": query, "start": start, "end": end, "step": step}

    async def fake_safe_call(awaitable):
        return await awaitable

    monkeypatch.setattr(metrics_route, "enforce_request_tenant", lambda req: req)
    monkeypatch.setattr(metrics_route, "get_provider", lambda tenant_id: DummyProvider())
    monkeypatch.setattr(metrics_route, "safe_call", fake_safe_call)
    monkeypatch.setattr(
        metrics_route.anomaly,
        "iter_series",
        lambda raw, query_hint=None: [
            ("metric-b", [1], [2.0]),
            ("metric-a", [2], [3.0]),
        ],
    )
    monkeypatch.setattr(
        metrics_route.anomaly,
        "detect",
        lambda metric, ts, vals, sensitivity: [types.SimpleNamespace(timestamp=3, metric=metric)]
        if metric == "metric-b"
        else [types.SimpleNamespace(timestamp=1, metric=metric)],
    )

    req = MetricRequest(tenant_id="tenant-a", query="up", start=1, end=5, step="30s", sensitivity=4.0)
    anomalies = await metrics_route.metric_anomalies(req)
    assert [item.timestamp for item in anomalies] == [1, 3]

    captured = []

    def fake_changepoint_detect(ts, vals, threshold_sigma=None, metric_name=None):
        if metric_name is not None:
            raise TypeError("legacy signature")
        captured.append((tuple(ts), tuple(vals), threshold_sigma))
        return [types.SimpleNamespace(timestamp=2)]

    monkeypatch.setattr(metrics_route, "changepoint_detect", fake_changepoint_detect)
    changepoints = await metrics_route.metric_changepoints(
        ChangepointRequest(tenant_id="tenant-a", query="up", start=1, end=5, step="30s", threshold_sigma=6.0)
    )

    assert [item.timestamp for item in changepoints] == [2, 2]
    assert captured == [((1,), (2.0,), 6.0), ((2,), (3.0,), 6.0)]


@pytest.mark.asyncio
async def test_correlation_and_trace_routes_cover_remaining_branches(monkeypatch):
    class DummyProvider:
        async def query_logs(self, query, start, end):
            return {"streams": [query, start, end]}

        async def query_traces(self, filters, start, end):
            return {"filters": filters, "start": start, "end": end}

    class DummyRegistry:
        async def get_state(self, tenant_id):
            return types.SimpleNamespace(weighted_confidence=lambda metric, log, trace: metric + log + trace)

    monkeypatch.setattr(correlation_route, "enforce_request_tenant", lambda req: req)
    monkeypatch.setattr(correlation_route, "build_log_query", lambda services, log_query: "joined-query")
    monkeypatch.setattr(correlation_route, "get_provider", lambda tenant_id: DummyProvider())
    monkeypatch.setattr(correlation_route, "DEFAULT_METRIC_QUERIES", ["default-metric"])

    async def fake_fetch_metrics(provider, queries, start, end, step):
        return [("q1", "metric-raw")]

    monkeypatch.setattr(correlation_route, "fetch_metrics", fake_fetch_metrics)
    monkeypatch.setattr(correlation_route.anomaly, "iter_series", lambda resp, query_hint=None: [("cpu", [1], [2.0])])
    monkeypatch.setattr(correlation_route.anomaly, "detect", lambda metric, ts, vals: [types.SimpleNamespace(name=metric)])
    monkeypatch.setattr(correlation_route.logs, "detect_bursts", lambda raw: [types.SimpleNamespace(stream="api")])
    monkeypatch.setattr(
        correlation_route,
        "correlate",
        lambda metric_anomalies, log_bursts_list, traces, window_seconds, weight_fn: [
            types.SimpleNamespace(
                window_start=1,
                window_end=2,
                confidence=weight_fn(0.5, 0.25, 0.0),
                signal_count=2,
                metric_anomalies=metric_anomalies,
                log_bursts=log_bursts_list,
            )
        ],
    )
    monkeypatch.setattr(
        correlation_route,
        "link_logs_to_metrics",
        lambda metric_anomalies, log_bursts_list: [
            types.SimpleNamespace(metric_name="cpu", log_stream="api", lag_seconds=3.0, strength=0.8)
        ],
    )
    monkeypatch.setattr(correlation_route, "get_registry", lambda: DummyRegistry())

    result = await correlation_route.correlate_signals(
        CorrelateRequest(
            tenant_id="tenant-a",
            start=1,
            end=5,
            step="30s",
            services=["api"],
            metric_queries=["custom-metric"],
            window_seconds=90,
        )
    )

    assert result == {
        "correlated_events": [{
            "window_start": 1,
            "window_end": 2,
            "confidence": 0.75,
            "signal_count": 2,
            "metric_anomaly_count": 1,
            "log_burst_count": 1,
        }],
        "log_metric_links": [{
            "metric_name": "cpu",
            "log_stream": "api",
            "lag_seconds": 3.0,
            "strength": 0.8,
        }],
    }

    async def fake_safe_call(awaitable):
        return await awaitable

    monkeypatch.setattr(traces_route, "enforce_request_tenant", lambda req: req)
    monkeypatch.setattr(traces_route, "get_provider", lambda tenant_id: DummyProvider())
    monkeypatch.setattr(traces_route, "safe_call", fake_safe_call)
    monkeypatch.setattr(traces_route.traces, "analyze", lambda raw, threshold: [{"raw": raw, "threshold": threshold}])

    traced = await traces_route.trace_anomalies(
        TraceRequest(tenant_id="tenant-a", start=3, end=9, service="checkout", apdex_threshold_ms=250.0)
    )
    unfiltered = await traces_route.trace_anomalies(
        TraceRequest(tenant_id="tenant-a", start=3, end=9, service=None, apdex_threshold_ms=100.0)
    )

    assert traced == [{
        "raw": {"filters": {"service.name": "checkout"}, "start": 3, "end": 9},
        "threshold": 250.0,
    }]
    assert unfiltered == [{
        "raw": {"filters": {}, "start": 3, "end": 9},
        "threshold": 100.0,
    }]


@pytest.mark.asyncio
async def test_lifespan_runs_database_setup_and_cleanup(monkeypatch):
    calls = []
    original_create_task = asyncio.create_task

    class ImmediateTask:
        def __init__(self, coro):
            self._task = original_create_task(coro)

        def cancel(self):
            self._task.cancel()

        def __await__(self):
            return self._task.__await__()

    async def fake_startup_recovery():
        calls.append("startup_recovery")

    async def fake_close_providers():
        calls.append("close_providers")

    async def fake_wait_for_all_bg(settings, tenant_id):
        calls.append(("wait_for_all_bg", tenant_id))

    async def fake_cleanup_loop():
        calls.append("cleanup_loop")

    monkeypatch.setattr(app_main.settings, "database_url", "sqlite:///tmp.db")
    monkeypatch.setattr(app_main.settings, "default_tenant_id", "tenant-z")
    monkeypatch.setattr(app_main, "init_database", lambda url: calls.append(("init_database", url)))
    monkeypatch.setattr(app_main, "init_db", lambda: calls.append("init_db"))
    monkeypatch.setattr(app_main.rca_job_service, "startup_recovery", fake_startup_recovery)
    monkeypatch.setattr(app_main, "_wait_for_all_bg", fake_wait_for_all_bg)
    monkeypatch.setattr(app_main, "_cleanup_loop", fake_cleanup_loop)
    monkeypatch.setattr(app_main, "close_providers", fake_close_providers)
    monkeypatch.setattr(app_main, "dispose_database", lambda: calls.append("dispose_database"))
    monkeypatch.setattr(app_main.asyncio, "create_task", lambda coro: ImmediateTask(coro))

    async with app_main.lifespan(app_main.app):
        await asyncio.sleep(0)

    assert calls == [
        ("init_database", "sqlite:///tmp.db"),
        "init_db",
        "startup_recovery",
        ("wait_for_all_bg", "tenant-z"),
        "cleanup_loop",
        "close_providers",
        "dispose_database",
    ]


@pytest.mark.asyncio
async def test_wait_for_all_bg_with_victoria_and_cleanup_loop(monkeypatch):
    calls = []

    settings = types.SimpleNamespace(
        logs_backend="none",
        metrics_backend="victoriametrics",
        traces_backend="none",
        loki_url="http://loki",
        mimir_url="http://mimir",
        victoriametrics_url="http://vm",
        tempo_url="http://tempo",
        startup_timeout=2,
    )

    async def fake_wait_for(name, url, timeout, headers=None, accept_status=(200,)):
        calls.append((name, url, headers, accept_status))

    async def fake_cleanup_retention():
        calls.append("cleanup_retention")
        raise asyncio.CancelledError()

    async def fake_sleep(seconds):
        calls.append(("sleep", seconds))

    monkeypatch.setattr(app_main, "wait_for", fake_wait_for)
    app_main._backend_ready = False
    app_main._backend_status = {}
    await app_main._wait_for_all_bg(settings, "tenant-a")

    monkeypatch.setattr(app_main.settings, "database_url", "sqlite:///tmp.db")
    monkeypatch.setattr(app_main.asyncio, "sleep", fake_sleep)
    monkeypatch.setattr(app_main.rca_job_service, "cleanup_retention", fake_cleanup_retention)

    with pytest.raises(asyncio.CancelledError):
        await app_main._cleanup_loop()

    assert calls == [
        (
            "victoriametrics",
            "http://vm/api/v1/label/__name__/values",
            {"X-Scope-OrgID": "tenant-a"},
            (200,),
        ),
        ("sleep", 300),
        "cleanup_retention",
    ]


def test_dunder_main_runs_uvicorn_with_ssl(monkeypatch):
    captured = {}

    monkeypatch.setenv("RESOLVER_SSL_ENABLED", "true")
    monkeypatch.setenv("RESOLVER_SSL_CERTFILE", "/tmp/cert.pem")
    monkeypatch.setenv("RESOLVER_SSL_KEYFILE", "/tmp/key.pem")
    monkeypatch.setenv("RESOLVER_HOST", "0.0.0.0")
    monkeypatch.setenv("RESOLVER_PORT", "9443")
    import config as config_module

    importlib.reload(config_module)
    monkeypatch.setitem(sys.modules, "uvicorn", types.SimpleNamespace(run=lambda app, **kwargs: captured.update({"app": app, **kwargs})))
    runpy.run_module("main", run_name="__main__")

    assert captured["app"] == "main:app"
    assert captured["host"] == "0.0.0.0"
    assert captured["port"] == 9443
    assert captured["ssl_certfile"] == "/tmp/cert.pem"
    assert captured["ssl_keyfile"] == "/tmp/key.pem"