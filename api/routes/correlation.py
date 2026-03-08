"""
Correlation routes for quick cross-signal temporal correlation without full RCA.

Copyright (c) 2026 Stefan Kumarasinghe
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
"""

from __future__ import annotations

import asyncio
from typing import Any, Dict

from fastapi import APIRouter, Depends

from api.requests import CorrelateRequest
from api.routes.common import get_provider
from api.routes.exception import handle_exceptions
from config import DEFAULT_METRIC_QUERIES
from engine import anomaly, logs
from engine.correlation import correlate, link_logs_to_metrics
from engine.fetcher import fetch_metrics
from engine.log_query import build_log_query
from engine.registry import get_registry
from services.security_service import enforce_request_tenant, require_permission_dependency

router = APIRouter(tags=["Correlation"])


@router.post(
    "/correlate",
    summary="Cross-signal temporal correlation without full RCA",
    dependencies=[Depends(require_permission_dependency("read:rca"))],
)
@handle_exceptions
async def correlate_signals(req: CorrelateRequest) -> Dict[str, Any]:
    req = enforce_request_tenant(req)
    log_query = build_log_query(req.services, req.log_query)
    provider = get_provider(req.tenant_id)
    all_queries = list(dict.fromkeys((req.metric_queries or []) + DEFAULT_METRIC_QUERIES))

    logs_raw, metrics_raw = await asyncio.gather(
        provider.query_logs(
            query=log_query,
            start=req.start * 1_000_000_000,
            end=req.end * 1_000_000_000,
        ),
        fetch_metrics(provider, all_queries, req.start, req.end, req.step),
        return_exceptions=True,
    )

    metric_anomalies = []
    if not isinstance(metrics_raw, Exception):
        for query_string, resp in metrics_raw:
            for metric_name, ts, vals in anomaly.iter_series(resp, query_hint=query_string):
                metric_anomalies.extend(anomaly.detect(metric_name, ts, vals))

    log_bursts_list = []
    if not isinstance(logs_raw, Exception):
        log_bursts_list = logs.detect_bursts(logs_raw)

    # compute confidence using tenant-specific signal weights if available
    state = await get_registry().get_state(req.tenant_id)
    events = correlate(
        metric_anomalies,
        log_bursts_list,
        [],
        window_seconds=req.window_seconds,
        weight_fn=state.weighted_confidence,
    )
    links = link_logs_to_metrics(metric_anomalies, log_bursts_list)

    return {
        "correlated_events": [
            {
                "window_start": e.window_start,
                "window_end": e.window_end,
                "confidence": e.confidence,
                "signal_count": e.signal_count,
                "metric_anomaly_count": len(e.metric_anomalies),
                "log_burst_count": len(e.log_bursts),
            }
            for e in events
        ],
        "log_metric_links": [
            {
                "metric_name": lk.metric_name,
                "log_stream": lk.log_stream,
                "lag_seconds": lk.lag_seconds,
                "strength": lk.strength,
            }
            for lk in links
        ],
    }
