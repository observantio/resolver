"""
Temporal correlation logic to identify related anomalies across different signals (metrics, logs, traces) based on their
occurrence within a configurable time window, and to compute a confidence score for the correlation based on the number
and types of signals involved, to assist in root cause analysis and incident investigation.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
"""

from __future__ import annotations

import math
import re
from collections.abc import Callable
from dataclasses import dataclass, field

from api.responses import LogBurst, MetricAnomaly, ServiceLatency
from config import settings


@dataclass
class CorrelatedEvent:
    window_start: float
    window_end: float
    metric_anomalies: list[MetricAnomaly] = field(default_factory=list)
    log_bursts: list[LogBurst] = field(default_factory=list)
    service_latency: list[ServiceLatency] = field(default_factory=list)
    signal_count: int = 0
    confidence: float = 0.0


def _overlap(a_start: float, a_end: float, b_start: float, b_end: float) -> bool:
    return a_start <= b_end and b_start <= a_end


def _tokenize(value: str) -> set[str]:
    raw = (value or "").strip().lower()
    if not raw:
        return set()
    for ch in "{}[]=,():|\"'":
        raw = raw.replace(ch, " ")
    for ch in ".-/":
        raw = raw.replace(ch, " ")
    return {part for part in raw.split() if part}


_SERVICE_LABEL_RE = re.compile(r"(?:service(?:\.name|_name)?|job)\s*=\s*([^,}]+)")


def _normalize_service(value: object) -> str:
    return str(value or "").strip().strip('"').strip("'").lower()


def _service_tokens_from_metric_name(metric_name: str) -> set[str]:
    text = str(metric_name or "")
    matches = _SERVICE_LABEL_RE.findall(text)
    services = {_normalize_service(match) for match in matches if _normalize_service(match)}
    if services:
        return services
    return set()


def _service_tokens_from_log_burst(burst: object) -> set[str]:
    services: set[str] = set()
    stream = getattr(burst, "stream", None)
    if isinstance(stream, dict):
        for key in ("service", "service_name", "service.name", "job"):
            token = _normalize_service(stream.get(key))
            if token:
                services.add(token)
    return services


def _latency_window(latency: ServiceLatency) -> tuple[float | None, float | None]:
    start = _safe_float(getattr(latency, "window_start", None))
    end = _safe_float(getattr(latency, "window_end", None))
    if start is not None and end is not None and end < start:
        start, end = end, start
    return start, end


def _safe_float(value: object) -> float | None:
    try:
        if value is None:
            return None
        if not isinstance(value, (str, int, float)):
            return None
        number = float(value)
        return None if math.isnan(number) else number
    except (TypeError, ValueError):
        return None


def correlate(
    metric_anomalies: list[MetricAnomaly],
    log_bursts: list[LogBurst],
    service_latency: list[ServiceLatency],
    window_seconds: float | None = None,
    *,
    weight_fn: Callable[[float, float, float], float] | None = None,
) -> list[CorrelatedEvent]:
    if window_seconds is None:
        window_seconds = settings.correlation_window_seconds

    anchor_candidates: list[object] = [a.timestamp for a in metric_anomalies]
    anchor_candidates.extend(getattr(b, "start", getattr(b, "window_start", None)) for b in log_bursts)
    anchor_times: list[float] = []
    for value in anchor_candidates:
        parsed = _safe_float(value)
        if parsed is not None:
            anchor_times.append(parsed)
    anchor_times.sort()

    if not anchor_times:
        return []

    events: list[CorrelatedEvent] = []
    used: set[float] = set()

    for anchor in anchor_times:
        if anchor in used:
            continue

        w_start = anchor - window_seconds
        w_end = anchor + window_seconds

        ma = [a for a in metric_anomalies if w_start <= a.timestamp <= w_end]
        lb = []
        for burst in log_bursts:
            burst_start = getattr(burst, "start", getattr(burst, "window_start", None))
            burst_end = getattr(burst, "end", getattr(burst, "window_end", None))
            burst_start = _safe_float(burst_start)
            burst_end = _safe_float(burst_end)
            if burst_start is None or burst_end is None:
                continue
            if _overlap(w_start, w_end, burst_start, burst_end):
                lb.append(burst)
        metric_services: set[str] = set()
        for anomaly in ma:
            metric_services.update(_service_tokens_from_metric_name(getattr(anomaly, "metric_name", "")))
        log_services: set[str] = set()
        for burst in lb:
            log_services.update(_service_tokens_from_log_burst(burst))
        correlated_services = metric_services | log_services

        sl = []
        for latency in service_latency:
            service_name = _normalize_service(getattr(latency, "service", ""))
            if not service_name:
                continue
            if not correlated_services:
                continue
            if service_name not in correlated_services:
                continue
            latency_start, latency_end = _latency_window(latency)
            if latency_start is None or latency_end is None:
                continue
            if _overlap(w_start, w_end, latency_start, latency_end):
                sl.append(latency)

        sig = len(ma) + len(lb) + len(sl)
        if sig < 2:
            continue

        metric_score = min(settings.correlation_score_max, len(ma) * settings.correlation_weight_time)
        log_score = min(settings.correlation_score_max, len(lb) * settings.correlation_weight_latency)
        trace_score = min(settings.correlation_errors_cap, len(sl) * settings.correlation_weight_errors)
        if weight_fn is not None:
            raw_conf = weight_fn(metric_score, log_score, trace_score)
        else:
            raw_conf = metric_score + log_score + trace_score
        confidence = round(min(settings.correlation_score_max, raw_conf), 3)

        events.append(
            CorrelatedEvent(
                window_start=w_start,
                window_end=w_end,
                metric_anomalies=ma,
                log_bursts=lb,
                service_latency=sl,
                signal_count=sig,
                confidence=confidence,
            )
        )

        for a in anchor_times:
            if w_start <= a <= w_end:
                used.add(a)

    return sorted(events, key=lambda e: e.confidence, reverse=True)
