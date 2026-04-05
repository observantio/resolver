"""
Causal inference routes for root cause analysis.

Copyright (c) 2026 Stefan Kumarasinghe Licensed under the Apache License, Version 2.0 (the "License"); you may not use
this file except in compliance with the License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
"""

from __future__ import annotations

from typing import Dict

import numpy as np
from fastapi import APIRouter, Depends, Query

from api.requests import AnalyzeRequest, CorrelateRequest
from api.routes.common import coerce_query_value, get_provider, safe_call
from api.routes.exception import handle_exceptions
from config import DEFAULT_METRIC_QUERIES, DEFAULT_SERVICE_NAME
from custom_types.json import JSONDict
from datasources.provider import DataSourceProvider
from engine import anomaly
from engine.causal import CausalGraph, bayesian_score, test_all_pairs
from engine.fetcher import fetch_metrics
from engine.registry import get_registry
from services.security_service import enforce_request_tenant, require_permission_dependency
from store import granger as granger_store

router = APIRouter(tags=["Causal"])


def _select_top_variance_series(series_map: Dict[str, list[float]], max_series: int) -> Dict[str, list[float]]:

    ranked: list[tuple[str, float]] = []
    for name, values in series_map.items():
        arr = np.array(values, dtype=float)
        finite = arr[np.isfinite(arr)]
        if finite.size < 12:
            continue
        variance = float(np.var(finite))
        if variance <= 0:
            continue
        ranked.append((name, variance))
    ranked.sort(key=lambda item: item[1], reverse=True)
    selected = {name for name, _ in ranked[:max_series]}
    return {name: values for name, values in series_map.items() if name in selected}


def _common_causes_for_roots(causal_graph: CausalGraph, roots: list[str]) -> Dict[str, list[str]]:
    common: Dict[str, list[str]] = {}
    for idx, root_a in enumerate(roots):
        for root_b in roots[idx + 1 :]:
            pair_key = f"{root_a}|{root_b}"
            common[pair_key] = causal_graph.find_common_causes(root_a, root_b)
    return common


async def _fetch_requested_metrics(provider: DataSourceProvider, req: CorrelateRequest) -> list[tuple[str, JSONDict]]:
    queries = list(dict.fromkeys((getattr(req, "metric_queries", None) or []) + DEFAULT_METRIC_QUERIES))
    return await safe_call(fetch_metrics(provider, queries, req.start, req.end, req.step))


@router.post(
    "/causal/granger",
    summary="Granger causality between metrics (bounded by default)",
    dependencies=[Depends(require_permission_dependency("read:rca"))],
)
@handle_exceptions
async def granger_causality(
    req: CorrelateRequest,
    limit: int = Query(default=100, ge=1, le=2000),
    min_strength: float = Query(default=0.05, ge=0.0, le=1.0),
    max_series: int = Query(default=25, ge=2, le=200),
    include_raw: bool = Query(default=False),
) -> JSONDict:
    limit = coerce_query_value(limit, int)
    min_strength = coerce_query_value(min_strength, float)
    max_series = coerce_query_value(max_series, int)
    include_raw = coerce_query_value(include_raw, bool)
    req = enforce_request_tenant(req)
    provider = get_provider(req.tenant_id)
    metrics_raw = await _fetch_requested_metrics(provider, req)

    series_map: Dict[str, list[float]] = {}
    for query_string, resp in metrics_raw:
        for metric_name, _, vals in anomaly.iter_series(resp, query_hint=query_string):
            series_key = f"{query_string}::{metric_name}"
            series_map[series_key] = vals

    selected_series = _select_top_variance_series(series_map, max_series=max_series)
    fresh_results = test_all_pairs(selected_series) if len(selected_series) >= 2 else []
    fresh_results = [item for item in fresh_results if float(item.strength) >= float(min_strength)]
    fresh_results = sorted(fresh_results, key=lambda item: float(item.strength), reverse=True)[:limit]

    service_label = req.services[0] if req.services else DEFAULT_SERVICE_NAME
    merged = await granger_store.save_and_merge(req.tenant_id, service_label, fresh_results)
    merged = [item for item in merged if float(item.get("strength", 0.0)) >= float(min_strength)]
    merged = sorted(merged, key=lambda item: float(item.get("strength", 0.0)), reverse=True)[:limit]
    warm_causal_pairs: list[JSONDict] = [
        {
            "cause_metric": item["cause_metric"],
            "effect_metric": item["effect_metric"],
            "max_lag": item["max_lag"],
            "f_statistic": item["f_statistic"],
            "p_value": item["p_value"],
            "is_causal": item["is_causal"],
            "strength": item["strength"],
        }
        for item in merged
    ]

    causal_graph = CausalGraph()
    causal_graph.from_granger_results(fresh_results)

    response: JSONDict = {
        "fresh_pairs": len(fresh_results),
        "warm_model_pairs": len(merged),
        "candidate_series": len(series_map),
        "selected_series": len(selected_series),
        "causal_pairs": [r.__dict__ for r in fresh_results],
        "warm_causal_pairs": warm_causal_pairs,
        "root_causes": causal_graph.root_causes(),
        "interventions": {
            root: causal_graph.simulate_intervention(root).__dict__ for root in causal_graph.root_causes()
        },
        "topological_order": causal_graph.topological_sort(),
        "common_causes_between_roots": _common_causes_for_roots(causal_graph, causal_graph.root_causes()),
    }
    if include_raw:
        raw_pairs = test_all_pairs(series_map) if len(series_map) >= 2 else []
        response["raw_causal_pairs"] = [r.__dict__ for r in raw_pairs]
    return response


@router.post(
    "/causal/bayesian",
    summary="Bayesian posterior over RCA categories given observed signals",
    dependencies=[Depends(require_permission_dependency("read:rca"))],
)
@handle_exceptions
async def bayesian_rca(req: AnalyzeRequest) -> JSONDict:
    req = enforce_request_tenant(req)
    deployment_events = await get_registry().events_in_window(req.tenant_id, req.start, req.end)
    scores = bayesian_score(
        has_deployment_event=bool(deployment_events),
        has_metric_spike=bool(req.metric_queries),
        has_log_burst=bool(req.log_query),
        has_latency_spike=bool(req.services),
        has_error_propagation=False,
    )
    return {"posteriors": [{"category": s.category.value, "posterior": s.posterior, "prior": s.prior} for s in scores]}
