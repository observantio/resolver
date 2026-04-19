"""
Test API routes for causal analysis endpoints, validating request handling, response formatting, and integration with
the analysis engine.

Copyright (c) 2026 Stefan Kumarasinghe.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. See http://www.apache.org/licenses/LICENSE-2.0 for details.
"""

from types import SimpleNamespace

import pytest

from api.requests import CorrelateRequest
from api.routes import causal as causal_route


class DummyProvider:
    async def query_metrics(self, query, start, end, step):
        return {
            "data": {
                "result": [
                    {
                        "metric": {"__name__": "shared_metric"},
                        "values": [
                            [1, "1"],
                            [2, "2"],
                            [3, "3"],
                            [4, "4"],
                            [5, "5"],
                            [6, "6"],
                            [7, "7"],
                            [8, "8"],
                            [9, "9"],
                            [10, "10"],
                            [11, "11"],
                            [12, "12"],
                        ],
                    }
                ]
            }
        }


@pytest.mark.asyncio
async def test_granger_causality_uses_unique_series_keys(monkeypatch):
    dummy = DummyProvider()
    monkeypatch.setattr(causal_route, "get_provider", lambda tid: dummy)
    monkeypatch.setattr(causal_route, "DEFAULT_METRIC_QUERIES", [])

    captured = {"count": 0, "keys": []}

    def fake_test_all_pairs(series_map, max_lag=None, p_threshold=None):
        captured["count"] = len(series_map)
        captured["keys"] = sorted(series_map.keys())
        return []

    async def fake_save_and_merge(tenant_id, service, fresh_results):
        return []

    monkeypatch.setattr(causal_route, "test_all_pairs", fake_test_all_pairs)
    monkeypatch.setattr(causal_route.granger_store, "save_and_merge", fake_save_and_merge)

    req = CorrelateRequest(
        tenant_id="tenant-a",
        start=1,
        end=100,
        step="15s",
        metric_queries=["q1", "q2"],
    )

    res = await causal_route.granger_causality(req)
    assert res["fresh_pairs"] == 0
    assert captured["count"] == 2
    assert captured["keys"][0] != captured["keys"][1]
    assert res["common_causes_between_roots"] == {}


@pytest.mark.asyncio
async def test_granger_causality_include_raw_pairs(monkeypatch):
    dummy = DummyProvider()
    monkeypatch.setattr(causal_route, "get_provider", lambda tid: dummy)
    monkeypatch.setattr(causal_route, "DEFAULT_METRIC_QUERIES", [])

    calls = {"count": 0}

    def fake_test_all_pairs(series_map, max_lag=None, p_threshold=None):
        calls["count"] += 1
        if calls["count"] == 1:
            return []
        return [
            SimpleNamespace(
                cause_metric="q1::shared_metric",
                effect_metric="q2::shared_metric",
                max_lag=2,
                f_statistic=5.0,
                p_value=0.01,
                is_causal=True,
                strength=0.8,
            )
        ]

    async def fake_save_and_merge(tenant_id, service, fresh_results):
        return []

    monkeypatch.setattr(causal_route, "test_all_pairs", fake_test_all_pairs)
    monkeypatch.setattr(causal_route.granger_store, "save_and_merge", fake_save_and_merge)

    req = CorrelateRequest(
        tenant_id="tenant-a",
        start=1,
        end=100,
        step="15s",
        metric_queries=["q1", "q2"],
    )

    res = await causal_route.granger_causality(req, include_raw=True)
    assert res["raw_causal_pairs"] == [
        {
            "cause_metric": "q1::shared_metric",
            "effect_metric": "q2::shared_metric",
            "max_lag": 2,
            "f_statistic": 5.0,
            "p_value": 0.01,
            "is_causal": True,
            "strength": 0.8,
        }
    ]
