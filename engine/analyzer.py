"""
Analyzer Module for Root Cause Analysis and Correlation of Anomalies

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
"""

from __future__ import annotations

import asyncio
import dataclasses
import logging
import math
import re
import time
from collections import defaultdict
from typing import Dict, List, Tuple, cast

import numpy as np

from datasources.provider import DataSourceProvider
from engine import anomaly, logs, rca, traces
from engine.baseline import compute as baseline_compute
from engine.causal import CausalGraph, bayesian_score, test_all_pairs
from engine.changepoint import detect as changepoint_detect, ChangePoint
from config import DEFAULT_METRIC_QUERIES, FORECAST_THRESHOLDS, SLO_ERROR_QUERY, SLO_TOTAL_QUERY, settings
from engine.correlation import correlate, link_logs_to_metrics
from engine.dedup import group_metric_anomalies
from engine.events.registry import DeploymentEvent, EventRegistry
from engine.fetcher import fetch_metrics
from engine.forecast import analyze_degradation, forecast
from engine.ml import cluster, rank
from engine.registry import get_registry
from engine.slo import evaluate as slo_evaluate
from engine.topology import DependencyGraph
from store import baseline as baseline_store, granger as granger_store
from api.requests import AnalyzeRequest
from api.responses import (
    AnalysisQuality,
    AnalysisReport,
    RootCause as RootCauseModel,
    SloBurnAlert as SloBurnAlertModel,
)
from engine.enums import Severity, Signal

log = logging.getLogger(__name__)
_TRACE_COUNT_FALLBACK_CAP = 10_000


def _overall_severity(*groups) -> Severity:
    best = Severity.low
    for group in groups:
        for item in group:
            if item.severity.weight() > best.weight():
                best = item.severity
    return best


def _summary(report: AnalysisReport) -> str:
    parts = []

    if report.metric_anomalies:
        parts.append(f"{len(group_metric_anomalies(report.metric_anomalies))} metric anomaly group(s)")
    if report.log_bursts:
        parts.append(f"{len(report.log_bursts)} log burst(s)")
    if report.log_patterns:
        hi_count = sum(p.count for p in report.log_patterns if p.severity.weight() >= 3)
        if hi_count:
            parts.append(f"{hi_count} high/critical log events")
    if report.service_latency:
        parts.append(f"{len(report.service_latency)} service(s) degraded")
    if report.error_propagation:
        parts.append(f"error propagation from {report.error_propagation[0].source_service}")
    if report.slo_alerts:
        parts.append(f"{len(report.slo_alerts)} SLO burn alert(s)")
    if report.change_points:
        parts.append(f"{len(report.change_points)} change point(s)")
    if report.forecasts:
        critical = sum(1 for f in report.forecasts if f.severity.weight() >= 4)
        if critical:
            parts.append(f"{critical} imminent breach(es) predicted")
    if report.degradation_signals:
        parts.append(f"{len(report.degradation_signals)} degrading metric(s)")

    if not parts:
        return "No anomalies detected in the analysis window."

    top = f" Top: {report.root_causes[0].hypothesis[:120]}..." if report.root_causes else ""
    return f"[{report.overall_severity.value.upper()}] {' | '.join(parts)}.{top}"


def _build_log_query(services: list[str] | None, requested_log_query: str | None) -> str:
    requested = (requested_log_query or "").strip()
    if requested:
        # Loki rejects selectors with empty-compatible regex.
        return re.sub(r'=~"\.\*"', '=~".+"', requested)
    if services:
        escaped = [re.escape(s) for s in services if s]
        if escaped:
            return '{service_name=~"' + "|".join(escaped) + '"}'
    return '{service_name=~".+"}'


_SERVICE_LABEL_KEYS = ("service", "service_name", "service.name", "job")


def _normalize_services(services: list[str] | None) -> set[str]:
    return {str(service or "").strip().lower() for service in (services or []) if str(service or "").strip()}


def _result_matches_services(result: object, services: set[str]) -> bool:
    if not services:
        return True
    if not isinstance(result, dict):
        return False
    metric = result.get("metric", {})
    if not isinstance(metric, dict):
        return False
    for key in _SERVICE_LABEL_KEYS:
        value = str(metric.get(key) or "").strip().lower()
        if value and value in services:
            return True
    return False


def _filter_metric_response_by_services(response: object, services: set[str]) -> object:
    if not services:
        return response
    if not isinstance(response, dict):
        return response
    data = response.get("data")
    if not isinstance(data, dict):
        return response
    results = data.get("result")
    if not isinstance(results, list):
        return response
    filtered = [item for item in results if _result_matches_services(item, services)]
    if len(filtered) == len(results):
        return response
    response_copy = dict(response)
    data_copy = dict(data)
    data_copy["result"] = filtered
    response_copy["data"] = data_copy
    return response_copy


def _to_root_cause_model(rc) -> RootCauseModel:
    def _normalize_signals(values: list) -> list[Signal]:
        normalized: list[Signal] = []
        for raw in values:
            if isinstance(raw, Signal):
                normalized.append(raw)
                continue
            text = str(raw).lower()
            if text.startswith("metric"):
                normalized.append(Signal.metrics)
            elif text.startswith("log"):
                normalized.append(Signal.logs)
            elif text.startswith("trace"):
                normalized.append(Signal.traces)
            elif text.startswith("event") or text.startswith("deploy"):
                normalized.append(Signal.events)
        return list(dict.fromkeys(normalized))

    def _normalize_payload(payload: dict) -> dict:
        signals = payload.get("contributing_signals")
        if isinstance(signals, list):
            payload["contributing_signals"] = _normalize_signals(signals)
        confidence: object = payload.get("confidence", 0.0)
        if isinstance(confidence, (int, float, str)):
            try:
                confidence_value = float(confidence)
            except ValueError:
                confidence_value = 0.0
        else:
            confidence_value = 0.0
        if not math.isfinite(confidence_value):
            confidence_value = 0.0
        payload["confidence"] = max(0.0, min(1.0, confidence_value))
        return payload

    if dataclasses.is_dataclass(rc) and not isinstance(rc, type):
        return RootCauseModel(**_normalize_payload(dataclasses.asdict(rc)))
    if isinstance(rc, dict):
        return RootCauseModel(**_normalize_payload(dict(rc)))
    return RootCauseModel.model_validate(rc)


def _build_compat_registry(deployment_events: list) -> EventRegistry:
    registry = EventRegistry()
    for e in deployment_events:
        registry.register(DeploymentEvent(
            service=e["service"],
            timestamp=e["timestamp"],
            version=e["version"],
            author=e.get("author", ""),
            environment=e.get("environment", "production"),
            source=e.get("source", "redis"),
            metadata=e.get("metadata", {}),
        ))
    return registry


def _series_key(query_string: str, metric_name: str) -> str:
    return f"{query_string}::{metric_name}"


def _trim_to_len(values: list[float], target_len: int) -> list[float]:
    if len(values) == target_len:
        return values
    return values[:target_len]


def _dedupe_metric_anomalies(items: list) -> list:
    selected: dict[tuple[str, int, str], object] = {}
    for item in items:
        key = (
            str(getattr(item, "metric_name", "metric")),
            int(round(float(getattr(item, "timestamp", 0.0)))),
            str(getattr(getattr(item, "change_type", None), "value", getattr(item, "change_type", "unknown"))),
        )
        current = selected.get(key)
        if current is None:
            selected[key] = item
            continue
        curr_sev = getattr(current, "severity", Severity.low).weight()
        next_sev = getattr(item, "severity", Severity.low).weight()
        if next_sev > curr_sev:
            selected[key] = item
            continue
        if next_sev == curr_sev:
            if abs(float(getattr(item, "z_score", 0.0))) > abs(float(getattr(current, "z_score", 0.0))):
                selected[key] = item
    return sorted(selected.values(), key=lambda a: (a.timestamp, a.metric_name))


def _dedupe_change_points(items: List[ChangePoint]) -> List[ChangePoint]:
    selected: dict[tuple[str, int, str], ChangePoint] = {}
    for item in items:
        key = (
            str(getattr(item, "metric_name", "metric")),
            int(round(float(item.timestamp))),
            str(getattr(item.change_type, "value", item.change_type)),
        )
        current = selected.get(key)
        if current is None or float(item.magnitude) > float(current.magnitude):
            selected[key] = item
    return sorted(selected.values(), key=lambda c: (c.timestamp, c.metric_name))


def _dedupe_by_metric_with_severity(items: list) -> list:
    selected: dict[str, object] = {}
    for item in items:
        metric_name = str(getattr(item, "metric_name", "metric")).strip() or "metric"
        current = selected.get(metric_name)
        if current is None:
            selected[metric_name] = item
            continue
        curr_sev = getattr(getattr(current, "severity", Severity.low), "weight", lambda: 0)()
        next_sev = getattr(getattr(item, "severity", Severity.low), "weight", lambda: 0)()
        if next_sev > curr_sev:
            selected[metric_name] = item
            continue
        if next_sev == curr_sev:
            curr_signal = abs(float(getattr(current, "degradation_rate", getattr(current, "slope_per_second", 0.0))))
            next_signal = abs(float(getattr(item, "degradation_rate", getattr(item, "slope_per_second", 0.0))))
            if next_signal > curr_signal:
                selected[metric_name] = item
    return sorted(
        selected.values(),
        key=lambda item: (
            -getattr(getattr(item, "severity", Severity.low), "weight", lambda: 0)(),
            str(getattr(item, "metric_name", "metric")),
        ),
    )


def _cap_list(items: list, limit: int, key_func, reverse: bool = True) -> list:
    capped_limit = max(1, int(limit))
    if len(items) <= capped_limit:
        return items
    return sorted(items, key=key_func, reverse=reverse)[:capped_limit]


def _limit_analyzer_output(
    *,
    metric_anomalies: list,
    change_points: List[ChangePoint],
    root_causes: list[RootCauseModel],
    ranked_causes: list,
    anomaly_clusters: list,
    granger_results: list,
    warnings: list[str],
) -> tuple[list, List[ChangePoint], list[RootCauseModel], list, list, list]:
    metric_anomalies_limited = _cap_list(
        metric_anomalies,
        settings.analyzer_max_metric_anomalies,
        key_func=lambda item: (
            getattr(getattr(item, "severity", Severity.low), "weight", lambda: 0)(),
            abs(float(getattr(item, "z_score", 0.0))),
            float(getattr(item, "timestamp", 0.0)),
        ),
    )
    if len(metric_anomalies_limited) < len(metric_anomalies):
        warnings.append(
            f"Metric anomalies capped to top {len(metric_anomalies_limited)} from {len(metric_anomalies)} "
            "by severity and z-score."
        )

    change_points_limited = _cap_list(
        change_points,
        settings.analyzer_max_change_points,
        key_func=lambda item: (float(getattr(item, "magnitude", 0.0)), float(getattr(item, "timestamp", 0.0))),
    )
    if len(change_points_limited) < len(change_points):
        warnings.append(
            f"Change points capped to top {len(change_points_limited)} from {len(change_points)} by magnitude."
        )

    root_causes_limited = _cap_list(
        root_causes,
        settings.analyzer_max_root_causes,
        key_func=lambda item: float(getattr(item, "confidence", 0.0)),
    )
    if len(root_causes_limited) < len(root_causes):
        warnings.append(f"Root causes capped to top {len(root_causes_limited)} by confidence.")

    ranked_limited = _cap_list(
        ranked_causes,
        settings.analyzer_max_root_causes,
        key_func=lambda item: float(getattr(item, "final_score", 0.0)),
    )

    clusters_limited = _cap_list(
        anomaly_clusters,
        settings.analyzer_max_clusters,
        key_func=lambda item: int(getattr(item, "size", 0)),
    )
    if len(clusters_limited) < len(anomaly_clusters):
        warnings.append(f"Anomaly clusters capped to top {len(clusters_limited)} by size.")

    granger_limited = _cap_list(
        granger_results,
        settings.analyzer_max_granger_pairs,
        key_func=lambda item: float(getattr(item, "strength", 0.0)),
    )
    if len(granger_limited) < len(granger_results):
        warnings.append(f"Granger pairs capped to top {len(granger_limited)} by strength.")

    return (
        metric_anomalies_limited,
        change_points_limited,
        root_causes_limited,
        ranked_limited,
        clusters_limited,
        granger_limited,
    )


def _signal_key(value: object) -> str:
    if isinstance(value, Signal):
        return value.value
    text = str(value or "").strip().lower()
    if text.startswith("metric"):
        return Signal.metrics.value
    if text.startswith("log"):
        return Signal.logs.value
    if text.startswith("trace"):
        return Signal.traces.value
    if text.startswith("event") or text.startswith("deploy"):
        return Signal.events.value
    return text


def _root_cause_signal_count(root_cause: RootCauseModel) -> int:
    signals = getattr(root_cause, "contributing_signals", []) or []
    keys = {_signal_key(signal) for signal in signals if _signal_key(signal)}
    keys.discard("")
    return len(keys)


def _root_cause_corroboration_summary(root_cause: RootCauseModel) -> str:
    count = _root_cause_signal_count(root_cause)
    signals = sorted({
        _signal_key(signal)
        for signal in (getattr(root_cause, "contributing_signals", []) or [])
        if _signal_key(signal)
    })
    if not signals:
        return "single-signal evidence"
    return f"{count} corroborating signal(s): {', '.join(signals)}"


def _build_selection_score_components(ranked_item: object, root_cause: RootCauseModel) -> dict[str, float]:
    components: dict[str, float] = {}
    for key, value in (
        ("rule_confidence", getattr(root_cause, "confidence", None)),
        ("ml_score", getattr(ranked_item, "ml_score", None)),
        ("final_score", getattr(ranked_item, "final_score", None)),
    ):
        try:
            number = float(value)
            if math.isfinite(number):
                components[key] = round(number, 6)
        except (TypeError, ValueError):
            continue

    importances = getattr(ranked_item, "feature_importance", None)
    if isinstance(importances, dict):
        for name, value in importances.items():
            try:
                number = float(value)
            except (TypeError, ValueError):
                continue
            if math.isfinite(number):
                components[f"feature_importance:{name}"] = round(number, 6)
    return components


def _compute_anomaly_density(metric_anomalies: list, duration_seconds: float) -> dict[str, float]:
    if not metric_anomalies:
        return {}
    hours = max(float(duration_seconds) / 3600.0, 1.0 / 60.0)
    counts: dict[str, int] = defaultdict(int)
    for anomaly_item in metric_anomalies:
        metric_name = str(getattr(anomaly_item, "metric_name", "metric")).strip() or "metric"
        counts[metric_name] += 1
    return {name: round(count / hours, 4) for name, count in counts.items()}


def _is_precision_profile() -> bool:
    profile = str(getattr(settings, "quality_gating_profile", "precision_strict_v1")).strip()
    return profile.lower().startswith("precision")


def _safe_float(value: object) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(parsed):
        return None
    return parsed


def _is_strongly_periodic_log_bursts(log_bursts: list) -> bool:
    if len(log_bursts) < 4:
        return False
    starts = [
        _safe_float(getattr(burst, "window_start", getattr(burst, "start", None)))
        for burst in log_bursts
    ]
    starts = sorted(value for value in starts if value is not None)
    if len(starts) < 4:
        return False
    deltas = [starts[idx] - starts[idx - 1] for idx in range(1, len(starts))]
    deltas = [delta for delta in deltas if delta > 0]
    if len(deltas) < 3:
        return False
    median = float(np.median(deltas))
    if median < 20.0 or median > 180.0:
        return False
    std = float(np.std(deltas))
    cv = std / median if median > 0 else float("inf")
    if cv > 0.25:
        return False
    band = median * 0.2
    in_band = sum(1 for delta in deltas if abs(delta - median) <= band)
    return (in_band / len(deltas)) >= 0.75


def _filter_log_bursts_for_precision_rca(
    *,
    log_bursts: list,
    log_patterns: list,
    suppression_counts: dict[str, int],
    warnings: list[str],
) -> list:
    if not log_bursts:
        return log_bursts
    if not _is_precision_profile():
        return log_bursts
    if not log_patterns:
        return log_bursts
    highest_pattern_severity = max(
        (getattr(pattern, "severity", Severity.low).weight() for pattern in log_patterns),
        default=Severity.low.weight(),
    )
    if highest_pattern_severity > Severity.low.weight():
        return log_bursts
    if not _is_strongly_periodic_log_bursts(log_bursts):
        return log_bursts
    suppressed = len(log_bursts)
    suppression_counts["low_signal_periodic_log_bursts"] = (
        suppression_counts.get("low_signal_periodic_log_bursts", 0) + suppressed
    )
    warnings.append(
        f"Quality gate suppressed {suppressed} periodic low-severity log burst(s) from RCA corroboration."
    )
    return []


def _apply_precision_quality_gates(
    *,
    metric_anomalies: list,
    change_points: List[ChangePoint],
    root_causes: list[RootCauseModel],
    ranked_causes: list,
    duration_seconds: float,
    suppression_counts: dict[str, int],
    warnings: list[str],
) -> tuple[list, List[ChangePoint], list[RootCauseModel], list, AnalysisQuality]:
    profile = str(getattr(settings, "quality_gating_profile", "precision_strict_v1")).strip() or "precision_strict_v1"
    is_precision = _is_precision_profile()
    hours = max(float(duration_seconds) / 3600.0, 1.0 / 60.0)

    if is_precision and metric_anomalies:
        max_density = max(0.0, float(getattr(settings, "quality_max_anomaly_density_per_metric_per_hour", 0.0)))
        if max_density > 0:
            keep_per_metric = max(1, int(math.ceil(max_density * hours)))
            by_metric: dict[str, list] = defaultdict(list)
            for item in metric_anomalies:
                metric_name = str(getattr(item, "metric_name", "metric")).strip() or "metric"
                by_metric[metric_name].append(item)
            filtered: list = []
            suppressed = 0
            for items in by_metric.values():
                if len(items) <= keep_per_metric:
                    filtered.extend(items)
                    continue
                ranked = sorted(
                    items,
                    key=lambda a: (
                        getattr(getattr(a, "severity", Severity.low), "weight", lambda: 0)(),
                        abs(float(getattr(a, "z_score", 0.0))),
                        abs(float(getattr(a, "mad_score", 0.0))),
                        float(getattr(a, "timestamp", 0.0)),
                    ),
                    reverse=True,
                )
                filtered.extend(ranked[:keep_per_metric])
                suppressed += len(items) - keep_per_metric
            metric_anomalies = sorted(filtered, key=lambda a: (a.timestamp, a.metric_name))
            if suppressed > 0:
                suppression_counts["density_suppressed_metric_anomalies"] = (
                    suppression_counts.get("density_suppressed_metric_anomalies", 0) + suppressed
                )
                warnings.append(
                    f"Quality gate suppressed {suppressed} metric anomaly(ies) above density cap "
                    f"{max_density}/metric/hour."
                )
    if is_precision and change_points:
        max_density_cp = max(
            0.0,
            float(getattr(settings, "quality_max_change_point_density_per_metric_per_hour", 0.0)),
        )
        if max_density_cp > 0:
            keep_per_metric_cp = max(1, int(math.ceil(max_density_cp * hours)))
            by_metric_cp: dict[str, list[ChangePoint]] = defaultdict(list)
            for item in change_points:
                metric_name = str(getattr(item, "metric_name", "metric")).strip() or "metric"
                by_metric_cp[metric_name].append(item)
            filtered_cp: List[ChangePoint] = []
            suppressed_cp = 0
            for items in by_metric_cp.values():
                if len(items) <= keep_per_metric_cp:
                    filtered_cp.extend(items)
                    continue
                ranked_cp = sorted(
                    items,
                    key=lambda c: (
                        float(getattr(c, "magnitude", 0.0)),
                        float(getattr(c, "timestamp", 0.0)),
                    ),
                    reverse=True,
                )
                filtered_cp.extend(ranked_cp[:keep_per_metric_cp])
                suppressed_cp += len(items) - keep_per_metric_cp
            change_points = sorted(filtered_cp, key=lambda c: (c.timestamp, c.metric_name))
            if suppressed_cp > 0:
                suppression_counts["density_suppressed_change_points"] = (
                    suppression_counts.get("density_suppressed_change_points", 0) + suppressed_cp
                )
                warnings.append(
                    f"Quality gate suppressed {suppressed_cp} change point(s) above density cap "
                    f"{max_density_cp}/metric/hour."
                )

    if root_causes:
        min_corr = max(1, int(getattr(settings, "quality_min_corroboration_signals", 2)))
        max_without = max(1, int(getattr(settings, "quality_max_root_causes_without_multisignal", 1)))
        low_conf_cutoff = max(float(getattr(settings, "rca_min_confidence_display", 0.05)), 0.10)

        if is_precision:
            filtered_root_causes: list[RootCauseModel] = []
            suppressed_low_conf = 0
            for cause in root_causes:
                if float(getattr(cause, "confidence", 0.0)) < low_conf_cutoff and len(root_causes) > 1:
                    suppressed_low_conf += 1
                    continue
                filtered_root_causes.append(cause)
            if filtered_root_causes:
                root_causes = filtered_root_causes
            if suppressed_low_conf > 0:
                suppression_counts["low_confidence_root_causes"] = (
                    suppression_counts.get("low_confidence_root_causes", 0) + suppressed_low_conf
                )
                warnings.append(
                    f"Quality gate suppressed {suppressed_low_conf} low-confidence root cause(s) below {low_conf_cutoff:.2f}."
                )

            multi_signal = [cause for cause in root_causes if _root_cause_signal_count(cause) >= min_corr]
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
            suppression_counts["suppressed_ranked_causes"] = suppression_counts.get("suppressed_ranked_causes", 0) + dropped_ranked

        for cause in root_causes:
            if not getattr(cause, "corroboration_summary", None):
                cause.corroboration_summary = _root_cause_corroboration_summary(cause)
            diagnostics = dict(getattr(cause, "suppression_diagnostics", {}) or {})
            diagnostics.setdefault("gating_profile", profile)
            signal_count = _root_cause_signal_count(cause)
            diagnostics.setdefault("signal_count", signal_count)
            diagnostics["min_corroboration_signals"] = min_corr
            diagnostics["meets_min_corroboration_signals"] = signal_count >= min_corr
            cause.suppression_diagnostics = diagnostics

    quality = AnalysisQuality(
        anomaly_density=_compute_anomaly_density(metric_anomalies, duration_seconds),
        suppression_counts={k: int(v) for k, v in suppression_counts.items() if int(v) > 0},
        gating_profile=profile,
        confidence_calibration_version=str(
            getattr(settings, "quality_confidence_calibration_version", "calib_2026_02_25")
        ),
    )
    return metric_anomalies, change_points, root_causes, ranked_causes, quality


async def _process_one_metric_series(
    req: AnalyzeRequest,
    query_string: str,
    metric_name: str,
    ts: list[float],
    vals: list[float],
    z_threshold: float,
    analysis_window_seconds: float,
):
    try:
        # result is persisted by store; value not used later
        _ = await baseline_store.compute_and_persist(req.tenant_id, metric_name, ts, vals, z_threshold)
    except Exception:
        # fallback compute also only triggers side‑effects
        _ = baseline_compute(ts, vals, z_threshold=z_threshold)

    metric_anomalies = anomaly.detect(metric_name, ts, vals, req.sensitivity)
    sigma_multiplier = float(z_threshold) if z_threshold and math.isfinite(float(z_threshold)) else float(
        settings.cusum_threshold_sigma
    )
    sigma_multiplier = max(1.0, sigma_multiplier)
    try:
        change_points = changepoint_detect(ts, vals, threshold_sigma=sigma_multiplier, metric_name=metric_name)
    except TypeError:
        # Backward-compatible path for monkeypatched/legacy detector signatures.
        change_points = changepoint_detect(ts, vals, sigma_multiplier)

    threshold = next((v for k, v in FORECAST_THRESHOLDS.items() if k in query_string), None)
    if threshold and analysis_window_seconds >= float(getattr(settings, "analyzer_forecast_min_window_seconds", 0.0)):
        fc = forecast(metric_name, ts, vals, threshold, req.forecast_horizon_seconds)
    else:
        fc = None

    if analysis_window_seconds >= float(getattr(settings, "analyzer_degradation_min_window_seconds", 0.0)):
        deg = analyze_degradation(metric_name, ts, vals)
    else:
        deg = None

    return metric_anomalies, change_points, fc, deg


async def _process_metrics(
    provider: DataSourceProvider,
    req: AnalyzeRequest,
    all_metric_queries: List[str],
    z_threshold: float,
    analysis_window_seconds: float,
) -> Tuple[list, List[ChangePoint], list, list, Dict[str, List[float]]]:
    metrics_raw = await fetch_metrics(provider, all_metric_queries, req.start, req.end, req.step)
    requested_services = _normalize_services(req.services)
    if requested_services:
        metrics_raw = [
            (query_string, cast(Dict[str, object], _filter_metric_response_by_services(resp, requested_services)))
            for query_string, resp in metrics_raw
        ]

    series_list: List[Tuple[str, str, list, list]] = [
        (query_string, metric_name, ts, vals)
        for query_string, resp in metrics_raw
        for metric_name, ts, vals in anomaly.iter_series(resp, query_hint=query_string)
    ]

    tasks = [
        _process_one_metric_series(
            req,
            query_string,
            metric_name,
            ts,
            vals,
            z_threshold,
            analysis_window_seconds,
        )
        for query_string, metric_name, ts, vals in series_list
    ]
    processed = await asyncio.gather(*tasks, return_exceptions=True)

    metric_anomalies: list = []
    change_points: List[ChangePoint] = []
    forecasts: list = []
    degradation_signals: list = []
    series_map: Dict[str, List[float]] = {}

    for (query_string, metric_name, _ts, vals), result in zip(series_list, processed):
        series_map[_series_key(query_string, metric_name)] = vals
        if isinstance(result, BaseException):
            log.warning("Metric stage failed for %s (%s): %s", metric_name, query_string, result)
            continue
        metric_stage_anomalies, metric_stage_changes, fc, deg = cast(tuple[list, list, object, object], result)
        metric_anomalies.extend(metric_stage_anomalies)
        change_points.extend(metric_stage_changes)
        if fc:
            forecasts.append(fc)
        if deg:
            degradation_signals.append(deg)

    return metric_anomalies, change_points, forecasts, degradation_signals, series_map


def _slo_series_pairs(err_raw, tot_raw, warnings: list[str]) -> list[tuple[list[float], list[float], list[float]]]:
    err_series = list(anomaly.iter_series(err_raw, query_hint=SLO_ERROR_QUERY))
    tot_series = list(anomaly.iter_series(tot_raw, query_hint=SLO_TOTAL_QUERY))

    if len(err_series) != len(tot_series):
        warnings.append(
            f"SLO series mismatch: errors={len(err_series)} totals={len(tot_series)}. "
            f"Using first {min(len(err_series), len(tot_series))} pair(s)."
        )

    pairs = []
    for idx in range(min(len(err_series), len(tot_series))):
        _, err_ts, err_vals = err_series[idx]
        _, _tot_ts, tot_vals = tot_series[idx]
        if len(err_vals) != len(tot_vals):
            n = min(len(err_vals), len(tot_vals))
            warnings.append(f"SLO sample length mismatch at pair {idx}: errors={len(err_vals)} totals={len(tot_vals)}.")
            err_vals = _trim_to_len(err_vals, n)
            tot_vals = _trim_to_len(tot_vals, n)
            err_ts = _trim_to_len(err_ts, n)
        if err_vals and tot_vals and err_ts:
            pairs.append((err_ts, err_vals, tot_vals))
    return pairs


def _select_granger_series(series_map: Dict[str, List[float]]) -> Dict[str, List[float]]:
    min_samples = max(2, int(settings.analyzer_granger_min_samples))
    max_series = max(2, int(settings.analyzer_granger_max_series))

    eligible: list[tuple[str, float]] = []
    for name, values in series_map.items():
        arr = np.array(values, dtype=float)
        finite = arr[np.isfinite(arr)]
        if finite.size < min_samples:
            continue
        var = float(np.var(finite))
        if var <= 0:
            continue
        eligible.append((name, var))

    eligible.sort(key=lambda x: x[1], reverse=True)
    selected_names = {name for name, _ in eligible[:max_series]}
    return {name: vals for name, vals in series_map.items() if name in selected_names}


async def run(provider: DataSourceProvider, req: AnalyzeRequest) -> AnalysisReport:
    started = time.perf_counter()
    registry = get_registry()
    tenant_id = req.tenant_id
    normalized_services = [str(service or "").strip() for service in (req.services or []) if str(service or "").strip()]
    req.services = normalized_services
    primary_service = normalized_services[0] if normalized_services else None
    warnings: list[str] = []
    suppression_counts: dict[str, int] = {}
    analysis_window_seconds = float(max(0, req.end - req.start))

    log_query = _build_log_query(req.services, req.log_query)
    trace_filters = {"service.name": primary_service} if primary_service else {}
    all_metric_queries = list(dict.fromkeys((req.metric_queries or []) + DEFAULT_METRIC_QUERIES))

    if req.sensitivity:
        z_threshold = 1.0 + req.sensitivity * settings.analyzer_sensitivity_factor
    else:
        z_threshold = settings.baseline_zscore_threshold

    fetch_started = time.perf_counter()
    try:
        logs_raw, traces_raw, slo_errors_raw, slo_total_raw = await asyncio.wait_for(
            asyncio.gather(
                provider.query_logs(
                    query=log_query,
                    start=req.start * 1_000_000_000,
                    end=req.end * 1_000_000_000,
                ),
                provider.query_traces(filters=trace_filters, start=req.start, end=req.end),
                provider.query_metrics(query=SLO_ERROR_QUERY, start=req.start, end=req.end, step=req.step),
                provider.query_metrics(query=SLO_TOTAL_QUERY, start=req.start, end=req.end, step=req.step),
                return_exceptions=True,
            ),
            timeout=float(settings.analyzer_fetch_timeout_seconds),
        )
    except TimeoutError:
        warnings.append(
            f"Fetch stage timed out after {settings.analyzer_fetch_timeout_seconds}s; "
            "continuing with best-effort analysis."
        )
        logs_raw = TimeoutError("logs fetch timeout")
        traces_raw = TimeoutError("traces fetch timeout")
        slo_errors_raw = TimeoutError("slo error fetch timeout")
        slo_total_raw = TimeoutError("slo total fetch timeout")
    log.debug("analyzer stage=fetch duration=%.4fs", time.perf_counter() - fetch_started)

    metrics_started = time.perf_counter()
    try:
        metric_anomalies, change_points, forecasts, degradation_signals, series_map = await asyncio.wait_for(
            _process_metrics(provider, req, all_metric_queries, z_threshold, analysis_window_seconds),
            timeout=float(settings.analyzer_metrics_timeout_seconds),
        )
    except TimeoutError:
        msg = (
            f"Metrics stage timed out after {settings.analyzer_metrics_timeout_seconds}s; "
            "returning partial report."
        )
        warnings.append(msg)
        log.warning(msg)
        metric_anomalies, change_points, forecasts, degradation_signals, series_map = [], [], [], [], {}
    except Exception as exc:
        msg = f"Metrics unavailable: {exc}"
        warnings.append(msg)
        log.warning(msg)
        metric_anomalies, change_points, forecasts, degradation_signals, series_map = [], [], [], [], {}
    raw_metric_anomaly_count = len(metric_anomalies)
    raw_change_point_count = len(change_points)
    metric_anomalies = _dedupe_metric_anomalies(metric_anomalies)
    change_points = _dedupe_change_points(change_points)
    forecasts = _dedupe_by_metric_with_severity(forecasts)
    degradation_signals = _dedupe_by_metric_with_severity(degradation_signals)
    if raw_metric_anomaly_count > len(metric_anomalies):
        suppression_counts["duplicate_metric_anomalies"] = raw_metric_anomaly_count - len(metric_anomalies)
        warnings.append(
            f"Deduplicated metric anomalies from {raw_metric_anomaly_count} to {len(metric_anomalies)} "
            "to reduce duplicate series noise."
        )
    if raw_change_point_count > len(change_points):
        suppression_counts["duplicate_change_points"] = raw_change_point_count - len(change_points)
        warnings.append(
            f"Deduplicated change points from {raw_change_point_count} to {len(change_points)} "
            "to reduce duplicate series noise."
        )
    log.debug("analyzer stage=metrics duration=%.4fs", time.perf_counter() - metrics_started)

    logs_started = time.perf_counter()
    log_bursts, log_patterns = [], []
    if isinstance(logs_raw, dict):
        log_entries = logs_raw.get("data", {}).get("result", [])
        if not log_entries and not (req.log_query or "").strip():
            fallback_queries: list[str] = []
            if req.services:
                escaped = [re.escape(s) for s in req.services if s]
                if escaped:
                    pattern = "|".join(escaped)
                    fallback_queries.extend(
                        [
                            '{service_name=~"' + pattern + '"}',
                            '{service=~"' + pattern + '"}',
                        ]
                    )
            else:
                fallback_queries.extend(
                    [
                        '{service_name=~".+"}',
                        '{service=~".+"}',
                    ]
                )
            fallback_queries.append("{}")

            seen_selectors: set[str] = set()
            filtered_fallbacks: list[str] = []
            for selector in fallback_queries:
                if selector == log_query or selector in seen_selectors:
                    continue
                seen_selectors.add(selector)
                filtered_fallbacks.append(selector)

            for selector in filtered_fallbacks:
                try:
                    fallback_logs = await provider.query_logs(
                        query=selector,
                        start=req.start * 1_000_000_000,
                        end=req.end * 1_000_000_000,
                    )
                except Exception as exc:
                    log.debug("Logs fallback selector failed query=%s error=%s", selector, exc)
                    continue
                if isinstance(fallback_logs, dict) and fallback_logs.get("data", {}).get("result"):
                    logs_raw = fallback_logs
                    log_entries = logs_raw.get("data", {}).get("result", [])
                    log.info("Logs selector fallback succeeded using query=%s", selector)
                    break

        if not log_entries:
            warnings.append("Logs query returned no entries in the selected window.")

        log_bursts = logs.detect_bursts(logs_raw)
        log_patterns = logs.analyze(logs_raw)
    elif isinstance(logs_raw, Exception):
        msg = f"Logs unavailable: {logs_raw}"
        warnings.append(msg)
        log.warning(msg)
    else:
        msg = f"Logs unavailable: unsupported response type {type(logs_raw).__name__}"
        warnings.append(msg)
        log.warning(msg)
    log.debug("analyzer stage=logs duration=%.4fs", time.perf_counter() - logs_started)

    traces_started = time.perf_counter()
    service_latency, error_propagation = [], []
    graph = DependencyGraph()
    if isinstance(traces_raw, dict):
        service_latency = traces.analyze(traces_raw, req.apdex_threshold_ms)
        error_propagation = traces.detect_propagation(traces_raw)
        graph.from_spans(traces_raw)
        topology_critical_paths: dict[str, list[str]] = {}
        if primary_service:
            latency_services = sorted({s.service for s in service_latency if getattr(s, "service", "")})
            for service in latency_services[:3]:
                path = graph.critical_path(primary_service, service)
                if path:
                    topology_critical_paths[f"{primary_service}->{service}"] = path
        if topology_critical_paths:
            log.debug("analyzer topology critical_paths=%s", topology_critical_paths)
        if not traces_raw.get("traces"):
            warnings.append("Trace query returned no traces; topology and propagation insights are limited.")
            try:
                fallback = await provider.query_traces(
                    filters=trace_filters,
                    start=req.start,
                    end=req.end,
                    limit=_TRACE_COUNT_FALLBACK_CAP + 1,
                )
                trace_ids = fallback.get("traces", []) if isinstance(fallback, dict) else []
                count = len(trace_ids) if isinstance(trace_ids, list) else 0
                if count > _TRACE_COUNT_FALLBACK_CAP:
                    warnings.append("Trace ID fallback count: 10000+ traces in selected window.")
                elif count > 0:
                    warnings.append(f"Trace ID fallback count: {count} traces in selected window.")
            except Exception as exc:
                warnings.append(f"Trace ID fallback count unavailable: {exc}")
    elif isinstance(traces_raw, Exception):
        msg = f"Traces unavailable: {traces_raw}"
        warnings.append(msg)
        log.warning(msg)
    else:
        msg = f"Traces unavailable: unsupported response type {type(traces_raw).__name__}"
        warnings.append(msg)
        log.warning(msg)
    log.debug("analyzer stage=traces duration=%.4fs", time.perf_counter() - traces_started)

    slo_started = time.perf_counter()
    slo_alerts_raw = []
    if not isinstance(slo_errors_raw, Exception) and not isinstance(slo_total_raw, Exception):
        requested_service_set = _normalize_services(req.services)
        if requested_service_set:
            slo_errors_raw = _filter_metric_response_by_services(slo_errors_raw, requested_service_set)
            slo_total_raw = _filter_metric_response_by_services(slo_total_raw, requested_service_set)
        for err_ts, err_vals, tot_vals in _slo_series_pairs(slo_errors_raw, slo_total_raw, warnings):
            slo_alerts_raw.extend(
                slo_evaluate(primary_service or "global", err_vals, tot_vals, err_ts, req.slo_target or 0.999)
            )
    else:
        warnings.append("SLO metrics unavailable for one or both queries.")
    slo_alerts = [SloBurnAlertModel(**dataclasses.asdict(a)) for a in slo_alerts_raw]
    log.debug("analyzer stage=slo duration=%.4fs", time.perf_counter() - slo_started)

    correlate_started = time.perf_counter()
    rca_log_bursts = _filter_log_bursts_for_precision_rca(
        log_bursts=log_bursts,
        log_patterns=log_patterns,
        suppression_counts=suppression_counts,
        warnings=warnings,
    )
    # Keep raw links for investigation UX; filtered bursts are used for RCA correlation/scoring only.
    log_metric_links = link_logs_to_metrics(metric_anomalies, log_bursts)
    # fetch tenant-specific weights used to compute confidence
    state = await registry.get_state(tenant_id)
    correlated_events = correlate(
        metric_anomalies,
        rca_log_bursts,
        service_latency,
        window_seconds=req.correlation_window_seconds,
        weight_fn=state.weighted_confidence,
    )
    anomaly_clusters = cluster(metric_anomalies)
    log.debug("analyzer stage=correlate duration=%.4fs", time.perf_counter() - correlate_started)

    causal_started = time.perf_counter()
    series_for_granger = _select_granger_series(series_map)
    granger_started = time.perf_counter()
    fresh_granger = test_all_pairs(series_for_granger, max_lag=settings.granger_max_lag) if len(series_for_granger) >= 2 else []
    granger_elapsed = time.perf_counter() - granger_started
    if granger_elapsed > float(settings.analyzer_causal_timeout_seconds):
        warnings.append(
            f"Causal granger stage exceeded target {settings.analyzer_causal_timeout_seconds}s "
            f"(actual {granger_elapsed:.2f}s)."
        )

    try:
        await asyncio.wait_for(
            granger_store.save_and_merge(tenant_id, primary_service or "global", fresh_granger),
            timeout=1.0,
        )
    except Exception as exc:
        warnings.append(f"Failed to persist granger results: {exc}")

    causal_graph = CausalGraph()
    causal_graph.from_granger_results(fresh_granger)
    common_cause_hints: dict[str, list[str]] = {}
    anomalous_metrics = sorted({a.metric_name for a in metric_anomalies if getattr(a, "metric_name", "")})
    if anomalous_metrics:
        metric_a = anomalous_metrics[0]
        metric_b = anomalous_metrics[1] if len(anomalous_metrics) >= 2 else anomalous_metrics[0]
        common_cause_hints[f"{metric_a}|{metric_b}"] = causal_graph.find_common_causes(metric_a, metric_b)
    if common_cause_hints:
        log.debug("analyzer causal common_cause_hints=%s", common_cause_hints)

    deployment_events = cast(list[dict], await registry.events_in_window(tenant_id, req.start, req.end))
    bayesian_scores = bayesian_score(
        has_deployment_event=bool(deployment_events),
        has_metric_spike=bool(metric_anomalies),
        has_log_burst=bool(rca_log_bursts),
        has_latency_spike=bool(service_latency),
        has_error_propagation=bool(error_propagation),
    )

    root_causes = rca.generate(
        metric_anomalies,
        rca_log_bursts,
        log_patterns,
        service_latency,
        error_propagation,
        correlated_events=correlated_events,
        graph=graph,
        event_registry=_build_compat_registry(deployment_events),
    )
    ranked_causes = rank(root_causes, correlated_events)
    pydantic_root_causes: list[RootCauseModel] = []
    ranked_valid: list = []
    hypothesis_to_ranked: dict[str, object] = {}
    for item in ranked_causes:
        try:
            root_cause_model = _to_root_cause_model(item.root_cause)
            pydantic_root_causes.append(root_cause_model)
            ranked_valid.append(item)
            hypothesis = str(root_cause_model.hypothesis)
            current = hypothesis_to_ranked.get(hypothesis)
            if current is None or float(getattr(item, "final_score", 0.0)) > float(getattr(current, "final_score", 0.0)):
                hypothesis_to_ranked[hypothesis] = item
        except Exception as exc:
            suppression_counts["invalid_root_cause_drops"] = suppression_counts.get("invalid_root_cause_drops", 0) + 1
            warnings.append(f"Dropped invalid root cause model during normalization: {exc}")
    ranked_causes = ranked_valid
    for cause in pydantic_root_causes:
        ranked_item = hypothesis_to_ranked.get(str(cause.hypothesis))
        if ranked_item is None:
            continue
        cause.selection_score_components = _build_selection_score_components(ranked_item, cause)
    (
        metric_anomalies,
        change_points,
        pydantic_root_causes,
        ranked_causes,
        anomaly_clusters,
        fresh_granger,
    ) = _limit_analyzer_output(
        metric_anomalies=metric_anomalies,
        change_points=change_points,
        root_causes=pydantic_root_causes,
        ranked_causes=ranked_causes,
        anomaly_clusters=anomaly_clusters,
        granger_results=fresh_granger,
        warnings=warnings,
    )
    metric_anomalies, change_points, pydantic_root_causes, ranked_causes, quality = _apply_precision_quality_gates(
        metric_anomalies=metric_anomalies,
        change_points=change_points,
        root_causes=pydantic_root_causes,
        ranked_causes=ranked_causes,
        duration_seconds=float(req.end - req.start),
        suppression_counts=suppression_counts,
        warnings=warnings,
    )
    log.debug("analyzer stage=causal duration=%.4fs", time.perf_counter() - causal_started)

    severity = _overall_severity(
        metric_anomalies, log_bursts, log_patterns,
        service_latency, slo_alerts, forecasts,
    )
    has_actionable_now = bool(
        metric_anomalies
        or log_bursts
        or log_patterns
        or service_latency
        or error_propagation
        or slo_alerts
        or pydantic_root_causes
    )
    if not has_actionable_now and (forecasts or degradation_signals or change_points):
        if severity.weight() > Severity.medium.weight():
            warnings.append(
                "Overall severity was capped at MEDIUM because only predictive signals were present "
                "without corroborating actionable anomalies."
            )
            severity = Severity.medium

    report = AnalysisReport(
        tenant_id=tenant_id,
        start=req.start,
        end=req.end,
        duration_seconds=req.end - req.start,
        metric_anomalies=metric_anomalies,
        log_bursts=log_bursts,
        log_patterns=log_patterns,
        service_latency=service_latency,
        error_propagation=error_propagation,
        root_causes=pydantic_root_causes,
        ranked_causes=ranked_causes,
        slo_alerts=slo_alerts,
        change_points=change_points,
        log_metric_links=log_metric_links,
        forecasts=forecasts,
        degradation_signals=degradation_signals,
        anomaly_clusters=anomaly_clusters,
        granger_results=fresh_granger,
        bayesian_scores=bayesian_scores,
        analysis_warnings=warnings,
        overall_severity=severity,
        summary="",
        quality=quality,
    )
    report.summary = _summary(report)
    log.info(
        "analyzer done tenant=%s service=%s duration=%.4fs warnings=%d",
        tenant_id,
        primary_service or "global",
        time.perf_counter() - started,
        len(warnings),
    )
    return report
