"""
Frequency-based burst detection logic for logs, analyzing log entry timestamps to identify periods of unusually high log activity, with severity categorization based on configured thresholds.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
"""

from __future__ import annotations

import re
from typing import Any, Dict, Iterator, List, Tuple

import numpy as np

from engine.enums import Severity
from api.responses import LogBurst

from config import settings

_BURST_RATIO_THRESHOLDS = [
    (thr, Severity(sev)) for thr, sev in settings.burst_ratio_thresholds
]

_ADVERSE_RE = re.compile(
    r"\b(fatal|panic|oom|killed|segfault|out of memory|error|err|exception|failed|failure|crash|timeout|unavailable|refused)\b",
    re.I,
)
_BENIGN_RE = re.compile(
    r"(background saving|db saved on disk|fork cow|changes in \d+ seconds|terminated with success|heartbeat|healthcheck|ready|started|ok\b|success)",
    re.I,
)


def _iter_entries(loki_response: Dict[str, Any]) -> Iterator[Tuple[float, str]]:
    for stream in loki_response.get("data", {}).get("result", []):
        for ts_ns, line in stream.get("values", []):
            yield float(ts_ns) / 1e9, line


def _is_benign_repetitive_window(lines: List[str]) -> bool:
    if len(lines) < 3:
        return False
    adverse = sum(1 for line in lines if _ADVERSE_RE.search(str(line)))
    if adverse > 0:
        return False
    benign = sum(1 for line in lines if _BENIGN_RE.search(str(line)))
    return (benign / len(lines)) >= 0.6


def detect_bursts(loki_response: Dict[str, Any], window_seconds: float | None = None) -> List[LogBurst]:
    if window_seconds is None:
        window_seconds = settings.logs_frequency_window_seconds
    entries = sorted(_iter_entries(loki_response), key=lambda x: x[0])
    if len(entries) < 2:
        return []

    timestamps = np.array([t for t, _ in entries])
    start, end = timestamps[0], timestamps[-1]
    total_duration = end - start
    if total_duration <= 0:
        return []

    baseline_rate = len(timestamps) / total_duration

    windows: List[Tuple[float, float, int]] = []
    i = 0
    while i < len(timestamps):
        w_start = timestamps[i]
        w_end = w_start + window_seconds
        end_idx = int(np.searchsorted(timestamps, w_end, side="left"))
        count = end_idx - i
        lines = [entry[1] for entry in entries[i:end_idx]]
        windows.append((w_start, w_end, count, _is_benign_repetitive_window(lines)))
        i += max(1, count)

    if not windows:
        return []

    bursts: List[LogBurst] = []
    for w_start, w_end, count, benign_window in windows:
        rate = count / window_seconds
        ratio = rate / baseline_rate if baseline_rate > 0 else 0.0
        severity = next(
            (s for threshold, s in _BURST_RATIO_THRESHOLDS if ratio >= threshold),
            None,
        )
        if severity is None:
            continue
        if benign_window:
            severity = Severity.low
        bursts.append(LogBurst(
            window_start=w_start,
            window_end=w_end,
            rate_per_second=round(rate, 3),
            baseline_rate=round(baseline_rate, 3),
            ratio=round(ratio, 2),
            severity=severity,
        ))

    return bursts
