"""
Correlation routes for quick cross-signal temporal correlation without full RCA.

Copyright (c) 2026 Stefan Kumarasinghe
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
"""

from __future__ import annotations

import asyncio
import re
from typing import Any, Dict, List
from fastapi import APIRouter, Depends
from api.routes.common import get_provider, safe_call
from api.routes.exception import handle_exceptions
from services.security_service import enforce_request_tenant, require_permission_dependency
from engine import anomaly, logs
from config import DEFAULT_METRIC_QUERIES
from engine.correlation import correlate, link_logs_to_metrics
from engine.fetcher import fetch_metrics
from api.requests import CorrelateRequest

router = APIRouter(tags=["Correlation"])


def _build_log_query(services: list[str] | None, requested_log_query: str | None) -> str:
    requested = (requested_log_query or "").strip()
    if requested:
        return re.sub(r'=~"\.\*"', '=~".+"', requested)
    if services:
        escaped = [re.escape(s) for s in services if s]
        if escaped:
            return '{service_name=~"' + "|".join(escaped) + '"}'
    return '{service_name=~".+"}'


@router.post(
    "/correlate",
    summary="Cross-signal temporal correlation without full RCA",
    dependencies=[Depends(require_permission_dependency("read:rca"))],
)
@handle_exceptions
async def correlate_signals(req: CorrelateRequest) -> Dict[str, Any]:
    req = enforce_request_tenant(req)
    log_query = _build_log_query(req.services, req.log_query)
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

    events = correlate(metric_anomalies, log_bursts_list, [], window_seconds=req.window_seconds)
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
