"""
Test API routes for SLO analysis endpoints, validating request handling, response formatting, and integration with the
analysis engine.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
"""

from types import SimpleNamespace

import pytest

from api.requests import SloRequest
from api.routes import slo as slo_route
from config import settings


class DummyProvider:
    def __init__(self):
        self.queries = []

    async def query_metrics(self, query, start, end, step):
        self.queries.append(query)
        return {"data": {"result": [{"metric": {}, "values": [[1, "0"]]}]}}


def dummy_slo_evaluate(service, err_vals, tot_vals, ts, target):
    return []


def dummy_remaining(service, err_vals, tot_vals, target):
    return None


@pytest.mark.asyncio
async def test_slo_burn_default_queries(monkeypatch):
    dummy = DummyProvider()
    monkeypatch.setattr(slo_route, "get_provider", lambda tid: dummy)
    monkeypatch.setattr(slo_route, "slo_evaluate", dummy_slo_evaluate)
    monkeypatch.setattr(slo_route, "remaining_minutes", dummy_remaining)

    req = SloRequest(tenant_id="t1", service="abc", start=0, end=1, step="1", target_availability=0.99)
    await slo_route.slo_burn(req)

    assert dummy.queries[0] == settings.slo_error_query_template.format(service="abc")
    assert dummy.queries[1] == settings.slo_total_query_template.format(service="abc")


@pytest.mark.asyncio
async def test_slo_burn_custom_queries_override(monkeypatch):
    dummy = DummyProvider()
    monkeypatch.setattr(slo_route, "get_provider", lambda tid: dummy)
    monkeypatch.setattr(slo_route, "slo_evaluate", dummy_slo_evaluate)
    monkeypatch.setattr(slo_route, "remaining_minutes", dummy_remaining)

    req = SloRequest(
        tenant_id="t1",
        service="abc",
        start=0,
        end=1,
        step="1",
        target_availability=0.99,
        error_query="errQ",
        total_query="totQ",
    )
    await slo_route.slo_burn(req)

    assert dummy.queries == ["errQ", "totQ"]


@pytest.mark.asyncio
async def test_slo_burn_handles_mismatched_series_lengths(monkeypatch):
    class MismatchProvider:
        async def query_metrics(self, query, start, end, step):
            if "5.." in query:
                return {
                    "data": {
                        "result": [
                            {"metric": {"__name__": "err_a"}, "values": [[1, "1"], [2, "2"], [3, "3"]]},
                            {"metric": {"__name__": "err_b"}, "values": [[1, "1"]]},
                        ]
                    }
                }
            return {
                "data": {
                    "result": [
                        {"metric": {"__name__": "tot_a"}, "values": [[1, "10"], [2, "10"]]},
                    ]
                }
            }

    seen = {"calls": 0}

    def tracking_eval(service, err_vals, tot_vals, ts, target):
        seen["calls"] += 1
        assert len(err_vals) == len(tot_vals) == len(ts)
        return []

    dummy = MismatchProvider()
    monkeypatch.setattr(slo_route, "get_provider", lambda tid: dummy)
    monkeypatch.setattr(slo_route, "slo_evaluate", tracking_eval)
    monkeypatch.setattr(slo_route, "remaining_minutes", dummy_remaining)

    req = SloRequest(tenant_id="t1", service="abc", start=0, end=10, step="1", target_availability=0.99)
    res = await slo_route.slo_burn(req)
    assert "burn_alerts" in res
    assert seen["calls"] == 1


@pytest.mark.asyncio
async def test_slo_burn_serializes_budget_status(monkeypatch):
    dummy = DummyProvider()
    monkeypatch.setattr(slo_route, "get_provider", lambda tid: dummy)
    monkeypatch.setattr(
        slo_route.anomaly,
        "iter_series",
        lambda raw, query_hint=None: [("metric", [1], [1.0])],
    )
    monkeypatch.setattr(
        slo_route,
        "slo_evaluate",
        lambda service, err_vals, tot_vals, ts, target: [SimpleNamespace(name="fast-burn")],
    )
    monkeypatch.setattr(
        slo_route,
        "remaining_minutes",
        lambda service, err_vals, tot_vals, target: SimpleNamespace(remaining=42, status="healthy"),
    )

    req = SloRequest(tenant_id="t1", service="abc", start=0, end=1, step="1", target_availability=0.99)
    res = await slo_route.slo_burn(req)

    assert res == {
        "burn_alerts": [{"name": "fast-burn"}],
        "budget_status": {"remaining": 42, "status": "healthy"},
    }
