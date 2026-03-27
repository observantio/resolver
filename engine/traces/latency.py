"""
Latency analysis for traces, including Apdex scoring and severity classification.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
"""

from __future__ import annotations

from collections import defaultdict
from typing import List, TypedDict

import numpy as np

from engine.enums import Severity
from api.responses import ServiceLatency
from custom_types.json import JSONDict
from config import settings


class LatencyBucket(TypedDict):
    durations: list[float]
    errors: int
    total: int
    op: str
    window_start: float | None
    window_end: float | None


def _to_seconds(value: object) -> float | None:
    if value is None or not isinstance(value, (int, float, str)):
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if not np.isfinite(numeric):
        return None

    # Heuristic conversion for unix timestamps encoded in ns/us/ms.
    converted = numeric
    if numeric > 1e17:
        converted = numeric / 1e9
    elif numeric > 1e14:
        converted = numeric / 1e6
    elif numeric > 1e11:
        converted = numeric / 1e3
    return converted


def _trace_window_seconds(trace: JSONDict, duration_ms: float) -> tuple[float | None, float | None]:
    start = None
    for key in (
        "startTimeUnixNano",
        "startTime",
        "rootSpanStartTimeUnixNano",
        "traceStartTimeUnixNano",
        "traceStartTime",
        "timestamp",
        "timeUnixNano",
    ):
        start = _to_seconds(trace.get(key))
        if start is not None:
            break

    end = None
    for key in ("endTimeUnixNano", "endTime", "traceEndTimeUnixNano", "traceEndTime"):
        end = _to_seconds(trace.get(key))
        if end is not None:
            break

    if start is not None and end is None and duration_ms >= 0:
        end = start + (duration_ms / 1000.0)
    if start is None and end is not None and duration_ms >= 0:
        start = end - (duration_ms / 1000.0)
    if start is not None and end is not None and end < start:
        start, end = end, start
    return start, end


def _apdex(durations_ms: np.ndarray, t_ms: float) -> float:
    if durations_ms.size == 0:
        return 1.0
    satisfied = (durations_ms <= t_ms).sum()
    tolerating = ((durations_ms > t_ms) & (durations_ms <= 4 * t_ms)).sum()
    score = (float(satisfied) + 0.5 * float(tolerating)) / float(durations_ms.size)
    return round(score, 4)

def _severity(p99: float, error_rate: float, apdex: float) -> Severity:
    score = 0.0
    if p99 >= settings.trace_latency_p99_critical:
        score += 0.5
    elif p99 >= settings.trace_latency_p99_high:
        score += 0.35
    elif p99 >= settings.trace_latency_p99_medium:
        score += 0.2

    if error_rate >= settings.trace_latency_error_critical:
        score += 0.4
    elif error_rate >= settings.trace_latency_error_high:
        score += 0.25
    elif error_rate >= settings.trace_latency_error_medium:
        score += 0.1

    if apdex < settings.trace_latency_apdex_poor:
        score += 0.1
    elif apdex < settings.trace_latency_apdex_marginal:
        score += 0.05

    return Severity.from_score(min(score, 1.0))


def analyze(tempo_response: JSONDict, apdex_t_ms: float | None = None) -> List[ServiceLatency]:
    if apdex_t_ms is None:
        apdex_t_ms = settings.trace_latency_apdex_t_ms

    buckets: dict[str, LatencyBucket] = defaultdict(
        lambda: {
            "durations": [],
            "errors": 0,
            "total": 0,
            "op": "",
            "window_start": None,
            "window_end": None,
        }
    )

    traces = tempo_response.get("traces")
    if not isinstance(traces, list):
        return []

    for trace in traces:
        if not isinstance(trace, dict):
            continue
        service_value = trace.get("rootServiceName")
        operation_value = trace.get("rootTraceName")
        duration_value = _to_seconds(trace.get("durationMs"))
        service = service_value if isinstance(service_value, str) else "unknown"
        operation = operation_value if isinstance(operation_value, str) else "unknown"
        duration_ms = duration_value if duration_value is not None else 0.0
        key = f"{service}::{operation}"

        bucket = buckets[key]
        bucket["durations"].append(duration_ms)
        bucket["total"] += 1
        bucket["op"] = operation
        start_s, end_s = _trace_window_seconds(trace, duration_ms)
        if start_s is not None:
            current_start = bucket["window_start"]
            bucket["window_start"] = start_s if current_start is None else min(float(current_start), start_s)
        if end_s is not None:
            current_end = bucket["window_end"]
            bucket["window_end"] = end_s if current_end is None else max(float(current_end), end_s)

        from engine.traces.common import iter_trace_spans, span_has_error

        if any(span_has_error(span) for span in iter_trace_spans(trace)):
            bucket["errors"] += 1

    results: List[ServiceLatency] = []

    for key, bucket in buckets.items():
        durations = np.array(bucket["durations"], dtype=float)
        if durations.size == 0:
            continue

        service = key.split("::")[0]
        p50, p95, p99 = np.percentile(durations, [50, 95, 99]).astype(float)
        error_rate = bucket["errors"] / bucket["total"]
        apdex_score = _apdex(durations, apdex_t_ms)
        sev = _severity(p99, error_rate, apdex_score)

        if sev == Severity.low:
            continue

        results.append(ServiceLatency(
            service=service,
            operation=bucket["op"],
            p50_ms=round(p50, 2),
            p95_ms=round(p95, 2),
            p99_ms=round(p99, 2),
            apdex=apdex_score,
            error_rate=round(error_rate, 4),
            sample_count=bucket["total"],
            severity=sev,
            window_start=round(float(bucket["window_start"]), 6) if bucket["window_start"] is not None else None,
            window_end=round(float(bucket["window_end"]), 6) if bucket["window_end"] is not None else None,
        ))

    results.sort(key=lambda s: s.severity.weight(), reverse=True)
    return results
