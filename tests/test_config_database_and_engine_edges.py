"""
Copyright (c) 2026 Stefan Kumarasinghe.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
"""

from __future__ import annotations

import importlib
import os
import sys
from typing import Any, cast
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from api.requests import AnalyzeRequest
from api.routes import causal as causal_route
from engine.causal.granger import GrangerResult
from engine.dedup.grouping import group_metric_anomalies
from engine.enums import ChangeType, Severity
from engine.events.models import DeploymentEvent
from engine.fetcher import _extract_metric_names, _scrape_and_fill, fetch_metrics
import database as database_module

baseline_compute_module = importlib.import_module("engine.baseline.compute")


def _reload_config_module():
    for module_name in ("config", "Resolvers.config"):
        if module_name in sys.modules:
            del sys.modules[module_name]
    return importlib.import_module("config")


def _base_production_env() -> dict[str, str]:
    return {
        "APP_ENV": "production",
        "RESOLVER_DATABASE_URL": "postgresql://safeuser:safePass_123@db:5432/resolver",
        "RESOLVER_EXPECTED_SERVICE_TOKEN": "resolver_expected_service_token_prod_12345",
        "RESOLVER_CONTEXT_VERIFY_KEY": "resolver_context_verify_key_prod_1234567890",
        "RESOLVER_CONTEXT_ISSUER": "watchdog-main",
        "RESOLVER_CONTEXT_AUDIENCE": "resolver",
        "RESOLVER_CONTEXT_ALGORITHMS": "HS256",
        "RESOLVER_CONTEXT_REPLAY_TTL_SECONDS": "180",
    }


def _anomaly(metric_name: str, timestamp: float, severity: Severity):
    return SimpleNamespace(
        metric_name=metric_name,
        timestamp=timestamp,
        severity=severity,
        value=1.0,
        change_type=ChangeType.SPIKE,
        z_score=1.0,
        mad_score=1.0,
        isolation_score=0.0,
        expected_range=(0.0, 2.0),
        description="x",
    )


class _ScrapeMetrics:
    def __init__(self, text: str = "", error: Exception | None = None):
        self._text = text
        self._error = error

    async def scrape(self):
        if self._error is not None:
            raise self._error
        return self._text


class _FetchProvider:
    def __init__(self, responses: dict[str, object], *, scrape_text: str = "", scrape_error: Exception | None = None):
        self._responses = responses
        self.metrics = _ScrapeMetrics(scrape_text, scrape_error)

    async def query_metrics(self, query, start, end, step):
        response = self._responses[query]
        if isinstance(response, Exception):
            raise response
        return response


def test_config_security_validation_edges():
    with patch.dict(os.environ, _base_production_env(), clear=False):
        module = _reload_config_module()
    assert module._parse_context_algorithms("") == ["HS256"]
    assert module._parse_context_algorithms("hs256, hs384") == ["HS256", "HS384"]

    env = _base_production_env()
    env["RESOLVER_DATABASE_URL"] = ""
    with patch.dict(os.environ, env, clear=False):
        with pytest.raises(ValueError, match="DATABASE_URL"):
            _reload_config_module()

    env = _base_production_env()
    env["RESOLVER_CONTEXT_REPLAY_TTL_SECONDS"] = "0"
    with patch.dict(os.environ, env, clear=False):
        with pytest.raises(ValueError, match="REPLAY_TTL"):
            _reload_config_module()

    env = _base_production_env()
    env["RESOLVER_CONTEXT_ISSUER"] = ""
    with patch.dict(os.environ, env, clear=False):
        with pytest.raises(ValueError, match="CONTEXT_ISSUER"):
            _reload_config_module()

    env = _base_production_env()
    env["RESOLVER_CONTEXT_AUDIENCE"] = ""
    with patch.dict(os.environ, env, clear=False):
        with pytest.raises(ValueError, match="CONTEXT_AUDIENCE"):
            _reload_config_module()


def test_config_secret_helpers_cover_strong_secret_path():
    module = importlib.import_module("config")
    assert module._normalized_secret("  Strong-Token-123  ") == "strong-token-123"
    assert module._is_weak_secret("Strong-Token-123") is False


def test_baseline_compute_and_score_paths(monkeypatch):
    monkeypatch.setattr(baseline_compute_module.settings, "baseline_min_samples", 6)
    monkeypatch.setattr(baseline_compute_module.settings, "baseline_seasonal_min_samples", 24)
    monkeypatch.setattr(baseline_compute_module.settings, "baseline_zscore_threshold", 2.0)

    small = baseline_compute_module.compute([0.0, 1.0, 2.0], [1.0, 2.0, 3.0])
    assert small.sample_count == 3
    assert small.seasonal_mean is None
    assert small.lower < small.mean < small.upper

    ts = [float(hour * 3600) for hour in range(24)]
    vals = [10.0 + (hour % 3) for hour in range(24)]
    seasonal = baseline_compute_module.compute(ts, vals, z_threshold=1.5)
    assert seasonal.sample_count == 24
    assert seasonal.seasonal_mean is not None
    assert seasonal.std > 0.0

    flagged, z_score = baseline_compute_module.score(20.0, seasonal)
    assert flagged is True
    assert z_score > 0.0

    assert baseline_compute_module.score(1.0, baseline_compute_module.Baseline(1.0, 0.0, 0.5, 1.5)) == (False, 0.0)


def test_group_metric_anomalies_groups_and_updates_representative(monkeypatch):
    monkeypatch.setattr(importlib.import_module("engine.dedup.grouping").settings, "dedup_time_window", 10.0)
    assert group_metric_anomalies([]) == []

    grouped = group_metric_anomalies(
        [
            _anomaly("cpu", 10.0, Severity.LOW),
            _anomaly("cpu", 15.0, Severity.CRITICAL),
            _anomaly("mem", 16.0, Severity.HIGH),
            _anomaly("cpu", 40.0, Severity.MEDIUM),
        ]
    )
    assert len(grouped) == 3
    assert grouped[0].count == 2
    assert grouped[0].representative.severity == Severity.CRITICAL

    collapsed = group_metric_anomalies(
        [_anomaly("cpu", 1.0, Severity.LOW), _anomaly("mem", 5.0, Severity.HIGH)],
        by_metric=False,
        time_window=10.0,
    )
    assert len(collapsed) == 1
    assert collapsed[0].representative.metric_name == "mem"


@pytest.mark.asyncio
async def test_fetcher_scrape_fallback_and_helper_paths():
    metric_names = _extract_metric_names("sum(rate(http_requests_total[5m])) by (service)")
    assert {"sum", "rate", "http_requests_total", "service"}.issubset(set(metric_names))

    provider = SimpleNamespace(metrics=object())
    assert await _scrape_and_fill(provider, ["up"], 1, 2) == []

    provider = SimpleNamespace(metrics=_ScrapeMetrics(error=ValueError("bad scrape")))
    assert await _scrape_and_fill(provider, ["up"], 1, 2) == []

    provider = _FetchProvider(
        {
            "up": {"data": {"result": []}},
            "cpu_usage": {"data": {"result": []}},
            "bad": RuntimeError("boom"),
            "weird": [1, 2, 3],
        },
        scrape_text="""
            # HELP ignored
            up 1
            cpu_usage{instance=\"a\"} 3.5
            invalid_metric nope
        """,
    )
    results = await fetch_metrics(provider, ["up", "cpu_usage", "bad", "weird"], 10, 20, "15s")
    assert [query for query, _ in results] == ["up", "cpu_usage"]
    assert results[0][1]["data"]["result"][0]["metric"]["__name__"] == "up"

    direct = _FetchProvider({"up": {"data": {"result": [{"metric": {}, "values": [[1, 1.0]]}]}}})
    returned = await fetch_metrics(direct, ["up"], 0, 1, "30s")
    assert returned == [("up", {"data": {"result": [{"metric": {}, "values": [[1, 1.0]]}]}})]


@pytest.mark.asyncio
async def test_causal_route_helpers_and_bayesian(monkeypatch):
    selected = causal_route._select_top_variance_series(
        {
            "flat": [1.0] * 12,
            "short": [1.0] * 10,
            "high": [float(index % 4) for index in range(12)],
            "higher": [float(index) for index in range(12)],
        },
        max_series=1,
    )
    assert list(selected) == ["higher"]

    graph = SimpleNamespace(find_common_causes=lambda a, b: [f"{a}-{b}"])
    assert causal_route._common_causes_for_roots(graph, ["svc-a", "svc-b", "svc-c"]) == {
        "svc-a|svc-b": ["svc-a-svc-b"],
        "svc-a|svc-c": ["svc-a-svc-c"],
        "svc-b|svc-c": ["svc-b-svc-c"],
    }

    async def fake_events_in_window(tenant_id, start, end):
        return [DeploymentEvent(service="api", timestamp=1.0, version="v1")]

    monkeypatch.setattr(causal_route, "get_registry", lambda: SimpleNamespace(events_in_window=fake_events_in_window))
    monkeypatch.setattr(
        causal_route,
        "bayesian_score",
        lambda **kwargs: [SimpleNamespace(category=SimpleNamespace(value="deployment"), posterior=0.9, prior=0.35)],
    )

    response = await causal_route.bayesian_rca(
        AnalyzeRequest(
            tenant_id="tenant-a", start=1, end=10, metric_queries=["up"], log_query="error", services=["api"]
        )
    )
    assert response == {"posteriors": [{"category": "deployment", "posterior": 0.9, "prior": 0.35}]}


def test_database_setup_session_and_connection_paths(monkeypatch):
    database_module.dispose_database()
    created_sql: list[str] = []
    disposed_admin: list[bool] = []

    database_module._ensure_postgres_database_exists("sqlite:///tmp.db")
    database_module._ensure_postgres_database_exists("postgresql://user:pass@db")

    with pytest.raises(RuntimeError, match="Database not initialized"):
        with database_module.get_db_session():
            pass

    with pytest.raises(RuntimeError, match="Database not initialized"):
        database_module.init_db()

    assert database_module.connection_test() is False

    class FakeScalarResult:
        def __init__(self, exists):
            self._exists = exists

        def scalar(self):
            return self._exists

    class FakeConn:
        def __init__(self, exists):
            self._exists = exists
            self.executed = []

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, stmt, params=None):
            self.executed.append((stmt, params))
            return FakeScalarResult(self._exists)

        def exec_driver_sql(self, sql):
            created_sql.append(sql)

    class FakeAdminEngine:
        def __init__(self, exists):
            self._exists = exists

        def connect(self):
            return FakeConn(self._exists)

        def dispose(self):
            disposed_admin.append(True)

    engine_calls = []
    exists_toggle = {"value": False}

    def fake_create_engine(url, **kwargs):
        engine_calls.append((str(url), kwargs))
        if kwargs.get("isolation_level") == "AUTOCOMMIT":
            return FakeAdminEngine(exists_toggle["value"])
        return SimpleNamespace(dispose=lambda: disposed_admin.append(False))

    monkeypatch.setattr(database_module, "create_engine", fake_create_engine)
    database_module._ensure_postgres_database_exists("postgresql://user:pass@db:5432/resolver")
    assert created_sql == ['CREATE DATABASE "resolver"']
    assert disposed_admin == [True]

    created_sql.clear()
    exists_toggle["value"] = True
    database_module._ensure_postgres_database_exists("postgresql://user:pass@db:5432/resolver")
    assert created_sql == []

    with pytest.raises(RuntimeError, match="Invalid database name"):
        database_module._ensure_postgres_database_exists("postgresql://user:pass@db:5432/bad-name")

    ensure_calls = []
    fake_engine = SimpleNamespace(dispose=lambda: disposed_admin.append(False), connect=lambda: FakeConn(True))
    fake_session = SimpleNamespace(
        commit=lambda: ensure_calls.append("commit"),
        rollback=lambda: ensure_calls.append("rollback"),
        close=lambda: ensure_calls.append("close"),
    )
    database_module.dispose_database()
    monkeypatch.setattr(database_module, "_ensure_postgres_database_exists", lambda url: ensure_calls.append(url))
    monkeypatch.setattr(database_module, "create_engine", lambda url, **kwargs: fake_engine)
    monkeypatch.setattr(database_module, "sessionmaker", lambda **kwargs: (lambda: fake_session))
    monkeypatch.setattr(
        database_module.Base.metadata, "create_all", lambda bind: ensure_calls.append(("create_all", bind))
    )

    database_module.init_database("sqlite:///tmp.db")
    database_module.init_database("sqlite:///tmp.db")
    assert ensure_calls == ["sqlite:///tmp.db"]

    with database_module.get_db_session() as session:
        assert session is fake_session
    assert ensure_calls[-2:] == ["commit", "close"]

    with pytest.raises(RuntimeError, match="boom"):
        with database_module.get_db_session():
            raise RuntimeError("boom")
    assert ensure_calls[-2:] == ["rollback", "close"]

    database_module.init_db()
    assert ("create_all", fake_engine) in ensure_calls
    assert database_module.connection_test() is True

    class BrokenEngine:
        def connect(self):
            raise database_module.SQLAlchemyError("down")

        def dispose(self):
            ensure_calls.append("disposed")

    database_module._ENGINE = BrokenEngine()
    assert database_module.connection_test() is False
    database_module.dispose_database()
    assert database_module._ENGINE is None
    assert database_module._SESSION_FACTORY is None


def test_database_session_factory_must_be_callable() -> None:
    database_module.dispose_database()

    class _DisposableEngine:
        def dispose(self) -> None:
            return None

    database_module._ENGINE = cast(Any, _DisposableEngine())
    database_module._SESSION_FACTORY = cast(Any, object())
    with pytest.raises(RuntimeError, match="Database not initialized"):
        with database_module.get_db_session():
            pass
    database_module.dispose_database()
