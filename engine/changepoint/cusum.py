"""
Cusum (Cumulative Sum) change point detection logic for identifying significant shifts in metric behavior, to assist in
early detection of anomalies and support root cause analysis.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List

import numpy as np

from engine.enums import ChangeType


@dataclass(frozen=True)
class ChangePoint:
    index: int
    timestamp: float
    value_before: float
    value_after: float
    magnitude: float
    change_type: ChangeType
    metric_name: str = "metric"


def _classify(before: float, after: float, std: float) -> ChangeType:
    from config import settings

    delta = after - before
    relative = abs(delta) / (abs(before) + 1e-9)
    if relative > settings.cusum_relative_cutoff:
        return ChangeType.SPIKE if delta > 0 else ChangeType.DROP
    if abs(delta) > 2 * std:
        return ChangeType.SHIFT
    return ChangeType.DRIFT


def _detect_oscillation(arr: np.ndarray, window: int | None = None) -> List[int]:
    from config import settings

    if window is None:
        window = settings.cusum_window
    sign_changes = np.diff(np.sign(np.diff(arr)))
    indices = np.where(np.abs(sign_changes) > 1)[0]
    if len(indices) < window // 2:
        return []
    density = len(indices) / len(arr)
    return list(indices) if density > settings.cusum_oscillation_density_cutoff else []


def detect(
    ts: List[float],
    vals: List[float],
    threshold_sigma: float | None = None,
    metric_name: str = "metric",
) -> List[ChangePoint]:
    from config import settings

    if threshold_sigma is None:
        threshold_sigma = settings.cusum_threshold_sigma
    if len(vals) < 10:
        return []

    arr = np.array(vals, dtype=float)
    mu = np.mean(arr)
    sigma = np.std(arr)
    if sigma == 0:
        return []

    oscillation_indices = set(_detect_oscillation(arr))

    k = settings.cusum_k * sigma
    h = threshold_sigma * sigma
    cusum_pos = cusum_neg = 0.0
    results: List[ChangePoint] = []

    for i in range(1, len(arr)):
        cusum_pos = max(0.0, cusum_pos + arr[i] - mu - k)
        cusum_neg = max(0.0, cusum_neg - arr[i] + mu - k)

        if cusum_pos > h or cusum_neg > h:
            before = float(np.mean(arr[max(0, i - 5) : i]))
            after = float(np.mean(arr[i : min(len(arr), i + 5)]))
            ctype = ChangeType.OSCILLATION if i in oscillation_indices else _classify(before, after, sigma)
            results.append(
                ChangePoint(
                    index=i,
                    metric_name=metric_name or "metric",
                    timestamp=ts[i],
                    value_before=round(before, 4),
                    value_after=round(after, 4),
                    magnitude=round(abs(after - before) / sigma, 3),
                    change_type=ctype,
                )
            )
            cusum_pos = cusum_neg = 0.0

    return results
