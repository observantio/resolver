"""
Shared quality-gate and scoring helpers for analyzer output.

Copyright (c) 2026 Stefan Kumarasinghe.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. See http://www.apache.org/licenses/LICENSE-2.0 for details.
"""

from __future__ import annotations

import math
from collections import defaultdict
from collections.abc import Sequence

import numpy as np

from api.responses import LogBurst, LogPattern, MetricAnomaly, RootCause as RootCauseModel
from config import settings
from engine.enums import Severity, Signal
from engine.ml import RankedCause


def signal_key(value: object) -> str:
    if isinstance(value, Signal):
        return value.value
    text = str(value or "").strip().lower()
    prefixes = (
        ("metric", Signal.METRICS.value),
        ("log", Signal.LOGS.value),
        ("trace", Signal.TRACES.value),
        ("event", Signal.EVENTS.value),
        ("deploy", Signal.EVENTS.value),
    )
    for prefix, mapped in prefixes:
        if text.startswith(prefix):
            return mapped
    return text


def root_cause_signal_count(root_cause: RootCauseModel) -> int:
    signals = getattr(root_cause, "contributing_signals", []) or []
    keys = {signal_key(signal) for signal in signals if signal_key(signal)}
    keys.discard("")
    return len(keys)


def root_cause_corroboration_summary(root_cause: RootCauseModel) -> str:
    count = root_cause_signal_count(root_cause)
    signals = sorted(
        {
            signal_key(signal)
            for signal in (getattr(root_cause, "contributing_signals", []) or [])
            if signal_key(signal)
        }
    )
    if not signals:
        return "single-signal evidence"
    return f"{count} corroborating signal(s): {', '.join(signals)}"


def build_selection_score_components(ranked_item: object, root_cause: RootCauseModel) -> dict[str, float]:
    components: dict[str, float] = {}
    for key, value in (
        ("rule_confidence", getattr(root_cause, "confidence", None)),
        ("ml_score", getattr(ranked_item, "ml_score", None)),
        ("final_score", getattr(ranked_item, "final_score", None)),
    ):
        if value is None:
            continue
        try:
            number = float(value)
            if math.isfinite(number):
                components[key] = round(number, 6)
        except (TypeError, ValueError):
            continue

    importances = getattr(ranked_item, "feature_importance", None)
    if isinstance(importances, dict):
        for name, value in importances.items():
            if not isinstance(value, (str, int, float)):
                continue
            try:
                number = float(value)
            except (TypeError, ValueError):
                continue
            if math.isfinite(number):
                components[f"feature_importance:{name}"] = round(number, 6)
    return components


def compute_anomaly_density(metric_anomalies: Sequence[MetricAnomaly], duration_seconds: float) -> dict[str, float]:
    if not metric_anomalies:
        return {}
    hours = max(float(duration_seconds) / 3600.0, 1.0 / 60.0)
    counts: dict[str, int] = defaultdict(int)
    for anomaly_item in metric_anomalies:
        metric_name = str(getattr(anomaly_item, "metric_name", "metric")).strip() or "metric"
        counts[metric_name] += 1
    return {name: round(count / hours, 4) for name, count in counts.items()}


def is_precision_profile() -> bool:
    profile = str(getattr(settings, "quality_gating_profile", "precision_strict_v1")).strip()
    return profile.lower().startswith("precision")


def safe_float(value: object) -> float | None:
    if not isinstance(value, (str, int, float)):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(parsed):
        return None
    return parsed


def is_strongly_periodic_log_bursts(log_bursts: list[LogBurst]) -> bool:
    is_periodic = False
    if len(log_bursts) >= 4:
        raw_starts = [safe_float(getattr(burst, "window_start", getattr(burst, "start", None))) for burst in log_bursts]
        starts: list[float] = sorted([value for value in raw_starts if value is not None])
        deltas = [starts[idx] - starts[idx - 1] for idx in range(1, len(starts))]
        deltas = [delta for delta in deltas if delta > 0]
        if len(starts) >= 4 and len(deltas) >= 3:
            median = float(np.median(deltas))
            if 20.0 <= median <= 180.0:
                std = float(np.std(deltas))
                cv = std / median if median > 0 else float("inf")
                if cv <= 0.25:
                    band = median * 0.2
                    in_band = sum(1 for delta in deltas if abs(delta - median) <= band)
                    is_periodic = (in_band / len(deltas)) >= 0.75
    return is_periodic


def filter_log_bursts_for_precision_rca(
    *,
    log_bursts: list[LogBurst],
    log_patterns: list[LogPattern],
    suppression_counts: dict[str, int],
    warnings: list[str],
) -> list[LogBurst]:
    highest_pattern_severity = max(
        (getattr(pattern, "severity", Severity.LOW).weight() for pattern in log_patterns),
        default=Severity.LOW.weight(),
    )
    should_suppress = (
        bool(log_bursts)
        and is_precision_profile()
        and bool(log_patterns)
        and highest_pattern_severity <= Severity.LOW.weight()
        and is_strongly_periodic_log_bursts(log_bursts)
    )
    if not should_suppress:
        return log_bursts
    suppressed = len(log_bursts)
    suppression_counts["low_signal_periodic_log_bursts"] = (
        suppression_counts.get("low_signal_periodic_log_bursts", 0) + suppressed
    )
    warnings.append(f"Quality gate suppressed {suppressed} periodic low-severity log burst(s) from RCA corroboration.")
    return []


def finalize_root_cause_metadata(
    root_causes: list[RootCauseModel],
    *,
    min_corr: int,
) -> None:
    for cause in root_causes:
        if not getattr(cause, "corroboration_summary", None):
            cause.corroboration_summary = root_cause_corroboration_summary(cause)
        diagnostics = dict(getattr(cause, "suppression_diagnostics", {}) or {})
        diagnostics.setdefault(
            "gating_profile",
            str(getattr(settings, "quality_gating_profile", "precision_strict_v1")).strip() or "precision_strict_v1",
        )
        signal_count = root_cause_signal_count(cause)
        diagnostics.setdefault("signal_count", signal_count)
        diagnostics["min_corroboration_signals"] = min_corr
        diagnostics["meets_min_corroboration_signals"] = signal_count >= min_corr
        cause.suppression_diagnostics = diagnostics


def apply_root_cause_quality_gates(
    root_causes: list[RootCauseModel],
    ranked_causes: list[RankedCause],
    *,
    suppression_counts: dict[str, int],
    warnings: list[str],
) -> tuple[list[RootCauseModel], list[RankedCause]]:
    if not root_causes:
        return root_causes, ranked_causes
    min_corr = max(1, int(getattr(settings, "quality_min_corroboration_signals", 2)))
    max_without = max(1, int(getattr(settings, "quality_max_root_causes_without_multisignal", 1)))
    low_conf_cutoff = max(float(getattr(settings, "rca_min_confidence_display", 0.05)), 0.10)

    if is_precision_profile():
        filtered_root_causes: list[RootCauseModel] = []
        suppressed_low_conf = 0
        for cause in root_causes:
            if float(getattr(cause, "confidence", 0.0)) < low_conf_cutoff and len(root_causes) > 1:
                suppressed_low_conf += 1
                continue
            filtered_root_causes.append(cause)
        root_causes = filtered_root_causes or root_causes
        if suppressed_low_conf > 0:
            suppression_counts["low_confidence_root_causes"] = (
                suppression_counts.get("low_confidence_root_causes", 0) + suppressed_low_conf
            )
            warnings.append(
                "Quality gate suppressed "
                f"{suppressed_low_conf} low-confidence root cause(s) below {low_conf_cutoff:.2f}."
            )

        multi_signal = [cause for cause in root_causes if root_cause_signal_count(cause) >= min_corr]
        if not multi_signal and len(root_causes) > max_without:
            suppressed_without_multi = len(root_causes) - max_without
            root_causes = root_causes[:max_without]
            suppression_counts["root_causes_without_multisignal"] = (
                suppression_counts.get("root_causes_without_multisignal", 0) + suppressed_without_multi
            )
            warnings.append(
                f"Quality gate suppressed {suppressed_without_multi} root cause(s) without multi-signal corroboration."
            )

    allowed_hypotheses = {str(cause.hypothesis) for cause in root_causes}
    ranked_before = len(ranked_causes)
    ranked_causes = [
        item
        for item in ranked_causes
        if str(getattr(getattr(item, "root_cause", None), "hypothesis", "")) in allowed_hypotheses
    ]
    dropped_ranked = ranked_before - len(ranked_causes)
    if dropped_ranked > 0:
        suppression_counts["suppressed_ranked_causes"] = (
            suppression_counts.get("suppressed_ranked_causes", 0) + dropped_ranked
        )
    finalize_root_cause_metadata(root_causes, min_corr=min_corr)
    return root_causes, ranked_causes
