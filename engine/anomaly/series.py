"""
Series iteration logic for processing Mimir query responses, extracting metric labels and corresponding timestamp- value
pairs, to facilitate downstream analysis and anomaly detection on time series data.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
"""

from __future__ import annotations

import logging
import re
from collections.abc import Iterator, Mapping
from typing import TypeAlias

log = logging.getLogger(__name__)

MetricRecord: TypeAlias = dict[str, object]
MimirResponse: TypeAlias = Mapping[str, object]
WrappedMimirResponse: TypeAlias = MimirResponse | tuple[object, MimirResponse]

_LABEL_PRIORITY = (
    "service",
    "service_name",
    "service.name",
    "job",
    "process",
    "process_name",
    "process.name",
    "process_executable_name",
    "process.executable.name",
    "process_command",
    "process.command",
    "process_command_line",
    "process.command_line",
    "process_pid",
    "process.pid",
    "pid",
    "instance",
    "pod",
    "namespace",
    "operation",
    "method",
    "status_code",
)
_GENERIC_METRIC_NAMES = {"metric", "unknown", "series"}
_PROMQL_TOKEN_RE = re.compile(r"[a-zA-Z_:][a-zA-Z0-9_:]*")
_PROMQL_EXCLUDED_TOKENS = {
    "sum",
    "rate",
    "irate",
    "avg",
    "min",
    "max",
    "count",
    "by",
    "without",
    "histogram_quantile",
    "quantile_over_time",
    "increase",
    "delta",
    "clamp_min",
    "clamp_max",
    "topk",
    "bottomk",
    "vector",
    "service",
    "status_code",
    "client",
    "server",
    "cpu",
    "le",
}


def _metric_hint_from_query(query_hint: str | None) -> str | None:
    text = str(query_hint or "").strip()
    if not text:
        return None
    tokens = _PROMQL_TOKEN_RE.findall(text)
    if not tokens:
        return None
    candidates: list[str] = [
        token
        for token in tokens
        if token.lower() not in _PROMQL_EXCLUDED_TOKENS and "_" in token and not token.startswith("__")
    ]
    if candidates:
        return candidates[0]
    return None


def _fallback_metric_name(metric: MetricRecord, query_hint: str | None) -> str:
    hinted = _metric_hint_from_query(query_hint)
    if hinted:
        return hinted
    for key in _LABEL_PRIORITY:
        value = metric.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return f"series_{key}"
    return "unknown_metric"


def iter_series(
    mimir_response: WrappedMimirResponse,
    query_hint: str | None = None,
) -> Iterator[tuple[str, list[float], list[float]]]:
    if isinstance(mimir_response, tuple):
        if len(mimir_response) == 2 and isinstance(mimir_response[1], dict):
            if query_hint is None and isinstance(mimir_response[0], str):
                query_hint = mimir_response[0]
            mimir_response = mimir_response[1]
        else:
            log.warning("iter_series received unexpected tuple shape: %r", mimir_response)
            return

    if not isinstance(mimir_response, dict):
        log.warning("iter_series expected dict, got %s", type(mimir_response).__name__)
        return

    data = mimir_response.get("data")
    if not isinstance(data, dict):
        log.warning("iter_series: 'data' is not a dict: %s", type(data).__name__)
        return

    results = data.get("result")
    if not isinstance(results, list):
        log.warning("iter_series: 'data.result' is not a list: %s", type(results).__name__)
        return

    for result in results:
        if not isinstance(result, dict):
            continue

        metric_raw = result.get("metric")
        metric = metric_raw if isinstance(metric_raw, dict) else {}
        base = str(metric.get("__name__") or "").strip()
        if not base or base.lower() in _GENERIC_METRIC_NAMES:
            base = _fallback_metric_name(metric, query_hint)
        label_parts: list[str] = []
        for key in _LABEL_PRIORITY:
            value = metric.get(key)
            if value is None:
                continue
            text = str(value).strip()
            if not text:
                continue
            label_parts.append(f"{key}={text}")
            if len(label_parts) >= 3:
                break
        label = f"{base}{{{','.join(label_parts)}}}" if label_parts else base

        pairs = result.get("values")
        if not isinstance(pairs, list) or not pairs:
            continue

        ts: list[float] = []
        vals: list[float] = []
        for p in pairs:
            if not isinstance(p, (list, tuple)) or len(p) < 2:
                continue
            try:
                ts.append(float(p[0]))
                vals.append(float(p[1]))
            except (ValueError, TypeError, IndexError):
                continue

        if len(ts) < 2 or len(vals) < 2:
            continue

        yield label, ts, vals
