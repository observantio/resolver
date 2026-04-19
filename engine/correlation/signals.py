"""
Linking logic for correlating log bursts to metric anomalies, providing functionality to associate log streams with
metric anomalies based on temporal proximity and strength of correlation, to support root cause analysis and incident
investigation.

Copyright (c) 2026 Stefan Kumarasinghe.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. See http://www.apache.org/licenses/LICENSE-2.0 for details.
"""

from __future__ import annotations

from dataclasses import dataclass

from api.responses import LogBurst, MetricAnomaly
from config import settings


@dataclass(frozen=True)
class LogMetricLink:
    metric_name: str
    metric_timestamp: float
    log_stream: str
    log_burst_start: float
    lag_seconds: float
    strength: float


def link_logs_to_metrics(
    metric_anomalies: list[MetricAnomaly],
    log_bursts: list[LogBurst],
    max_lag_seconds: float | None = None,
) -> list[LogMetricLink]:
    if max_lag_seconds is None:
        max_lag_seconds = settings.max_lag_seconds
    links: list[LogMetricLink] = []

    for anomaly in metric_anomalies:
        for burst in log_bursts:
            burst_start = float(getattr(burst, "window_start", getattr(burst, "start", 0.0)))
            log_stream = str(getattr(burst, "stream", "unknown"))
            lag = anomaly.timestamp - burst_start
            if 0 <= lag <= max_lag_seconds:
                strength = round(1.0 - (lag / max_lag_seconds), 3)
                links.append(
                    LogMetricLink(
                        metric_name=anomaly.metric_name,
                        metric_timestamp=anomaly.timestamp,
                        log_stream=log_stream,
                        log_burst_start=burst_start,
                        lag_seconds=round(lag, 1),
                        strength=strength,
                    )
                )

    return sorted(links, key=lambda link: link.strength, reverse=True)
