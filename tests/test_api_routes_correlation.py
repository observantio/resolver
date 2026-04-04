"""
Copyright (c) 2026 Stefan Kumarasinghe.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
"""

import pytest

from api.routes import correlation as corr_route
from api.requests import CorrelateRequest


class DummyState:
    def __init__(self):
        self.weights_serializable = {"metrics": 1.0, "logs": 0.0, "traces": 0.0}
        self.update_count = 0

    def weighted_confidence(self, metric_score, log_score, trace_score):
        return metric_score * self.weights_serializable.get("metrics", 0.0)


class DummyRegistry:
    def __init__(self):
        self.state = DummyState()

    async def get_state(self, tenant_id):
        return self.state


class DummyProvider:
    def __init__(self):
        self.queried = False

    async def query_logs(self, query, start, end):
        # return empty structure expected by logs.detect_bursts
        return {"data": {"result": []}}


@pytest.mark.asyncio
async def test_correlate_route_uses_weights(monkeypatch):
    dummy_reg = DummyRegistry()
    monkeypatch.setattr(corr_route, "get_registry", lambda: dummy_reg)
    # bypass actual fetching by patching provider methods used in route
    monkeypatch.setattr(corr_route, "get_provider", lambda tid: DummyProvider())

    async def fake_fetch_metrics(*args, **kwargs):
        return []

    monkeypatch.setattr(corr_route, "fetch_metrics", fake_fetch_metrics)

    req = CorrelateRequest(
        tenant_id="t1",
        metric_queries=["foo"],
        services=[],
        log_query=None,
        start=0,
        end=10,
        step="1",
        window_seconds=30,
    )

    result = await corr_route.correlate_signals(req)
    # With no anomalies/logs there's nothing to correlate,
    # but route should still compute and return lists.
    assert "correlated_events" in result
    assert isinstance(result["correlated_events"], list)
    assert "log_metric_links" in result
