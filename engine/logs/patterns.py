"""
Pattern recognition logic for logs, including normalization, severity classification, and entropy-based uniqueness
scoring to identify common log patterns and their characteristics.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
"""

from __future__ import annotations

import math
import re
from collections import Counter, defaultdict
from collections.abc import Iterator, Mapping
from typing import TypedDict

from api.responses import LogPattern
from config import settings
from engine.enums import Severity

_NOISE = re.compile(settings.logs_noise_regex, re.I)

_SEVERITY_RE = {
    Severity.CRITICAL: re.compile(r"\b(fatal|panic|oom|killed|segfault|out of memory)\b", re.I),
    Severity.HIGH: re.compile(r"\b(error|err|exception|failed|failure|crash|timeout|unavailable|refused)\b", re.I),
    Severity.MEDIUM: re.compile(r"\b(warn|warning|slow|retry|retrying|degraded|circuit)\b", re.I),
}


class PatternBucket(TypedDict):
    count: int
    first: float
    last: float
    severity: Severity
    sample: str
    tokens: list[str]


def _iter_entries(loki_response: Mapping[str, object]) -> Iterator[tuple[float, str]]:
    data = loki_response.get("data")
    if not isinstance(data, dict):
        return
    streams = data.get("result")
    if not isinstance(streams, list):
        return
    for stream in streams:
        if not isinstance(stream, dict):
            continue
        values = stream.get("values")
        if not isinstance(values, list):
            continue
        for entry in values:
            if not isinstance(entry, (list, tuple)) or len(entry) < 2:
                continue
            ts_ns, line = entry[0], entry[1]
            yield float(ts_ns) / 1e9, line


def _normalize(line: str) -> str:
    return re.sub(r"\s+", " ", _NOISE.sub("<_>", line)).strip()[: settings.logs_normalized_length_cutoff]


def _classify(line: str) -> Severity:
    for severity in (Severity.CRITICAL, Severity.HIGH, Severity.MEDIUM):
        if _SEVERITY_RE[severity].search(line):
            return severity
    return Severity.LOW


def _entropy(tokens: list[str]) -> float:
    if not tokens:
        return 0.0
    counts = Counter(tokens)
    total = len(tokens)
    return -sum((c / total) * math.log2(c / total) for c in counts.values())


def analyze(loki_response: Mapping[str, object]) -> list[LogPattern]:
    buckets: dict[str, PatternBucket] = defaultdict(
        lambda: {
            "count": 0,
            "first": float("inf"),
            "last": float("-inf"),
            "severity": Severity.LOW,
            "sample": "",
            "tokens": [],
        }
    )

    for ts, line in _iter_entries(loki_response):
        key = _normalize(line)
        b = buckets[key]
        b["count"] += 1
        b["first"] = min(b["first"], ts)
        b["last"] = max(b["last"], ts)
        if not b["sample"]:
            b["sample"] = line[: settings.logs_sample_snippet]
        sev = _classify(line)
        if sev.weight() > b["severity"].weight():
            b["severity"] = sev
        if len(b["tokens"]) < settings.logs_token_cap:
            b["tokens"].extend(key.split())

    results: list[LogPattern] = []
    for pattern, b in buckets.items():
        if b["first"] == float("inf"):
            continue
        duration = max(b["last"] - b["first"], settings.logs_min_duration)
        results.append(
            LogPattern(
                pattern=pattern,
                count=b["count"],
                first_seen=b["first"],
                last_seen=b["last"],
                rate_per_minute=round(b["count"] / (duration / 60), 4),
                entropy=round(_entropy(b["tokens"]), 4),
                severity=b["severity"],
                sample=b["sample"],
            )
        )

    results.sort(key=lambda p: (p.severity.weight(), p.count), reverse=True)
    return results[:100]
