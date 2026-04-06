"""
Analyzer Module for Root Cause Analysis and Correlation of Anomalies.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
"""

from __future__ import annotations

from collections.abc import Sequence

from config import settings
from engine.enums import Severity
from engine.slo.models import SloBurnAlert


def _get_windows() -> list[tuple[str, float, float, Severity]]:
    windows: list[tuple[str, float, float, Severity]] = []
    for label, window_s, thr, sev in settings.slo_burn_windows:
        sev_enum = Severity.LOW
        if isinstance(sev, Severity):
            sev_enum = sev
        elif isinstance(sev, str):
            try:
                sev_enum = Severity(sev)
            except ValueError:
                sev_enum = Severity.__members__.get(sev.upper(), Severity.LOW)

        windows.append((label, float(window_s), float(thr), sev_enum))
    return windows


__all__ = ["SloBurnAlert", "evaluate"]


def evaluate(
    service: str,
    error_counts: Sequence[float],
    total_counts: Sequence[float],
    ts: Sequence[float],
    target_availability: float = settings.slo_default_target_availability,
) -> list[SloBurnAlert]:
    if not error_counts or not total_counts or len(ts) < 2:
        return []

    if len(error_counts) != len(total_counts):
        n = min(len(error_counts), len(total_counts))
        error_counts = error_counts[:n]
        total_counts = total_counts[:n]

    duration = max(0.0, ts[-1] - ts[0])
    total = sum(total_counts)
    errors = sum(error_counts)

    if total == 0:
        return []

    error_rate = errors / total
    allowed_error_rate = 1.0 - target_availability
    if allowed_error_rate <= 0:
        return []

    burn_rate = error_rate / allowed_error_rate
    alerts: list[SloBurnAlert] = []

    for label, window_s, threshold, sev in _get_windows():
        if duration < window_s * 0.5:
            continue
        if burn_rate >= threshold:
            consumed = min(100.0, (burn_rate * duration) / float(settings.slo_month_seconds) * 100.0)
            alerts.append(
                SloBurnAlert(
                    service=service,
                    window_label=label,
                    error_rate=round(error_rate, 6),
                    burn_rate=round(burn_rate, 3),
                    budget_consumed_pct=round(consumed, 2),
                    severity=sev,
                )
            )
            break

    return alerts
