"""
Analyzer Helpers.
Copyright (c) 2026 Stefan Kumarasinghe
Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
"""

from __future__ import annotations

import asyncio
import dataclasses
import logging
import math
from collections import defaultdict
from collections.abc import Callable, Sequence
from typing import Any, TypeAlias, TypeVar

import httpx
import numpy as np

from api.requests import AnalyzeRequest
from api.responses import (
    AnalysisQuality,
    LogBurst,
    LogPattern,
    MetricAnomaly,
    MetricSeriesDistributionStats,
)
from api.responses import (
    RootCause as RootCauseModel,
)
from config import FORECAST_THRESHOLDS, SLO_ERROR_QUERY, SLO_TOTAL_QUERY, settings
from custom_types.json import JSONDict
from datasources.provider import DataSourceProvider
from engine import anomaly
from engine.analyze.filters import (
    filter_metric_response_by_services as _filter_metric_response_by_services,
)
from engine.analyze.filters import (
    normalize_services as _normalize_services,
)
from engine.analyze.series import select_granger_series as _select_granger_series_impl
from engine.analyze.series import slo_series_pairs as _slo_series_pairs_impl
from engine.anomaly.stats import compute_series_distribution_stats
from engine.baseline import compute as baseline_compute
from engine.causal.granger import GrangerResult
from engine.changepoint import ChangePoint
from engine.changepoint import detect as changepoint_detect
from engine.enums import Severity, Signal
from engine.events.registry import DeploymentEvent, EventRegistry
from engine.fetcher import fetch_metrics
from engine.forecast import analyze_degradation, forecast
from engine.forecast.degradation import DegradationSignal
from engine.forecast.trajectory import TrajectoryForecast
from engine.ml import AnomalyCluster, RankedCause
from store import baseline as baseline_store
from store.events import StoredEvent

log = logging.getLogger(__name__)

_RECOVERABLE_ANALYSIS_ERRORS = (
    asyncio.TimeoutError,
    httpx.HTTPError,
    OSError,
    RuntimeError,
    TypeError,
    ValueError,
)

_ItemT = TypeVar("_ItemT")
_MetricItemT = TypeVar("_MetricItemT")
_SortKey: TypeAlias = tuple[float | int | str, ...] | float | int | str


@dataclasses.dataclass(frozen=True)
class AnalyzerOutputInputs:
    metric_anomalies: list[MetricAnomaly]
    change_points: list[ChangePoint]
    root_causes: list[RootCauseModel]
    ranked_causes: list[RankedCause]
    anomaly_clusters: list[AnomalyCluster]
    granger_results: list[GrangerResult]
    warnings: list[str]


@dataclasses.dataclass(frozen=True)
class PrecisionQualityGateInputs:
    metric_anomalies: list[MetricAnomaly]
    change_points: list[ChangePoint]
    root_causes: list[RootCauseModel]
    ranked_causes: list[RankedCause]
    duration_seconds: float
    suppression_counts: dict[str, int]
    warnings: list[str]


@dataclasses.dataclass(frozen=True)
class MetricSeriesJob:
    req: AnalyzeRequest
    query_string: str
    metric_name: str
    ts: list[float]
    vals: list[float]
    z_threshold: float
    analysis_window_seconds: float


def _to_root_cause_model(rc: object) -> RootCauseModel:
    def _normalize_signals(values: list[object]) -> list[Signal]:
        normalized: list[Signal] = []
        for raw in values:
            if isinstance(raw, Signal):
                normalized.append(raw)
                continue
            text = str(raw).lower()
            if text.startswith("metric"):
                normalized.append(Signal.METRICS)
            elif text.startswith("log"):
                normalized.append(Signal.LOGS)
            elif text.startswith("trace"):
                normalized.append(Signal.TRACES)
            elif text.startswith("event") or text.startswith("deploy"):
                normalized.append(Signal.EVENTS)
        return list(dict.fromkeys(normalized))

    def _normalize_payload(payload: dict[str, object]) -> dict[str, object]:
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
        return RootCauseModel.model_validate(_normalize_payload(dataclasses.asdict(rc)))
    if isinstance(rc, dict):
        return RootCauseModel.model_validate(_normalize_payload(dict(rc)))
    return RootCauseModel.model_validate(rc)


def _build_compat_registry(deployment_events: list[StoredEvent]) -> EventRegistry:
    registry = EventRegistry()
    for e in deployment_events:
        metadata_raw = e.get("metadata", {})
        metadata = metadata_raw if isinstance(metadata_raw, dict) else {}
        registry.register(
            DeploymentEvent(
                service=str(e["service"]),
                timestamp=e["timestamp"],
                version=str(e["version"]),
                author=str(e.get("author", "")),
                environment=str(e.get("environment", "production")),
                source=str(e.get("source", "redis")),
                metadata={str(key): str(value) for key, value in metadata.items()},
            )
        )
    return registry


def _series_key(query_string: str, metric_name: str) -> str:
    return f"{query_string}::{metric_name}"


def _dedupe_metric_anomalies(items: list[MetricAnomaly]) -> list[MetricAnomaly]:
    selected: dict[tuple[str, int, str], MetricAnomaly] = {}
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
        curr_sev = getattr(current, "severity", Severity.LOW).weight()
        next_sev = getattr(item, "severity", Severity.LOW).weight()
        if next_sev > curr_sev:
            selected[key] = item
            continue
        if next_sev == curr_sev:  # pragma: no branch
            if abs(float(getattr(item, "z_score", 0.0))) > abs(float(getattr(current, "z_score", 0.0))):
                selected[key] = item
    return sorted(selected.values(), key=lambda a: (a.timestamp, a.metric_name))


def _trim_to_len(values: list[float], target_len: int) -> list[float]:
    if len(values) == target_len:
        return values
    return values[:target_len]


def _dedupe_change_points(items: list[ChangePoint]) -> list[ChangePoint]:
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


def _dedupe_by_metric_with_severity(items: list[_MetricItemT]) -> list[_MetricItemT]:
    selected: dict[str, _MetricItemT] = {}
    for item in items:
        metric_name = str(getattr(item, "metric_name", "metric")).strip() or "metric"
        current = selected.get(metric_name)
        if current is None:
            selected[metric_name] = item
            continue
        curr_sev = getattr(getattr(current, "severity", Severity.LOW), "weight", lambda: 0)()
        next_sev = getattr(getattr(item, "severity", Severity.LOW), "weight", lambda: 0)()
        if next_sev > curr_sev:
            selected[metric_name] = item
            continue
        if next_sev == curr_sev:  # pragma: no branch
            curr_signal = abs(float(getattr(current, "degradation_rate", getattr(current, "slope_per_second", 0.0))))
            next_signal = abs(float(getattr(item, "degradation_rate", getattr(item, "slope_per_second", 0.0))))
            if next_signal > curr_signal:
                selected[metric_name] = item
    return sorted(
        selected.values(),
        key=lambda item: (
            -getattr(getattr(item, "severity", Severity.LOW), "weight", lambda: 0)(),
            str(getattr(item, "metric_name", "metric")),
        ),
    )


def _cap_list(
    items: list[_ItemT],
    limit: int,
    key_func: Callable[[_ItemT], _SortKey],
    reverse: bool = True,
) -> list[_ItemT]:
    capped_limit = max(1, int(limit))
    if len(items) <= capped_limit:
        return items
    return sorted(items, key=key_func, reverse=reverse)[:capped_limit]


def _limit_analyzer_output(
    inputs: AnalyzerOutputInputs | None = None,
    **legacy_kwargs: Any,
) -> tuple[
    list[MetricAnomaly],
    list[ChangePoint],
    list[RootCauseModel],
    list[RankedCause],
    list[AnomalyCluster],
    list[GrangerResult],
]:
    if inputs is None:
        inputs = AnalyzerOutputInputs(
            metric_anomalies=legacy_kwargs.get("metric_anomalies", []),
            change_points=legacy_kwargs.get("change_points", []),
            root_causes=legacy_kwargs.get("root_causes", []),
            ranked_causes=legacy_kwargs.get("ranked_causes", []),
            anomaly_clusters=legacy_kwargs.get("anomaly_clusters", []),
            granger_results=legacy_kwargs.get("granger_results", []),
            warnings=legacy_kwargs.get("warnings", []),
        )
    metric_anomalies_limited = _cap_list(
        inputs.metric_anomalies,
        settings.analyzer_max_metric_anomalies,
        key_func=lambda item: (
            getattr(getattr(item, "severity", Severity.LOW), "weight", lambda: 0)(),
            abs(float(getattr(item, "z_score", 0.0))),
            float(getattr(item, "timestamp", 0.0)),
        ),
    )
    if len(metric_anomalies_limited) < len(inputs.metric_anomalies):
        inputs.warnings.append(
            f"Metric anomalies capped to top {len(metric_anomalies_limited)} from {len(inputs.metric_anomalies)} "
            "by severity and z-score."
        )

    change_points_limited = _cap_list(
        inputs.change_points,
        settings.analyzer_max_change_points,
        key_func=lambda item: (float(getattr(item, "magnitude", 0.0)), float(getattr(item, "timestamp", 0.0))),
    )
    if len(change_points_limited) < len(inputs.change_points):
        inputs.warnings.append(
            f"Change points capped to top {len(change_points_limited)} from {len(inputs.change_points)} by magnitude."
        )

    root_causes_limited = _cap_list(
        inputs.root_causes,
        settings.analyzer_max_root_causes,
        key_func=lambda item: float(getattr(item, "confidence", 0.0)),
    )
    if len(root_causes_limited) < len(inputs.root_causes):
        inputs.warnings.append(f"Root causes capped to top {len(root_causes_limited)} by confidence.")

    ranked_limited = _cap_list(
        inputs.ranked_causes,
        settings.analyzer_max_root_causes,
        key_func=lambda item: float(getattr(item, "final_score", 0.0)),
    )

    clusters_limited = _cap_list(
        inputs.anomaly_clusters,
        settings.analyzer_max_clusters,
        key_func=lambda item: int(getattr(item, "size", 0)),
    )
    if len(clusters_limited) < len(inputs.anomaly_clusters):
        inputs.warnings.append(f"Anomaly clusters capped to top {len(clusters_limited)} by size.")

    granger_limited = _cap_list(
        inputs.granger_results,
        settings.analyzer_max_granger_pairs,
        key_func=lambda item: float(getattr(item, "strength", 0.0)),
    )
    if len(granger_limited) < len(inputs.granger_results):
        inputs.warnings.append(f"Granger pairs capped to top {len(granger_limited)} by strength.")

    ma, cp, rc = metric_anomalies_limited, change_points_limited, root_causes_limited
    return ma, cp, rc, ranked_limited, clusters_limited, granger_limited


def _signal_key(value: object) -> str:
    if isinstance(value, Signal):
        return value.value
    text = str(value or "").strip().lower()
    if text.startswith("metric"):
        return Signal.METRICS.value
    if text.startswith("log"):
        return Signal.LOGS.value
    if text.startswith("trace"):
        return Signal.TRACES.value
    if text.startswith("event") or text.startswith("deploy"):
        return Signal.EVENTS.value
    return text


def _root_cause_signal_count(root_cause: RootCauseModel) -> int:
    signals = getattr(root_cause, "contributing_signals", []) or []
    keys = {_signal_key(signal) for signal in signals if _signal_key(signal)}
    keys.discard("")
    return len(keys)


def _root_cause_corroboration_summary(root_cause: RootCauseModel) -> str:
    count = _root_cause_signal_count(root_cause)
    signals = sorted(
        {
            _signal_key(signal)
            for signal in (getattr(root_cause, "contributing_signals", []) or [])
            if _signal_key(signal)
        }
    )
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


def _compute_anomaly_density(metric_anomalies: Sequence[MetricAnomaly], duration_seconds: float) -> dict[str, float]:
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
    if not isinstance(value, (str, int, float)):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(parsed):
        return None
    return parsed


def _is_strongly_periodic_log_bursts(log_bursts: list[LogBurst]) -> bool:
    if len(log_bursts) < 4:
        return False
    raw_starts = [_safe_float(getattr(burst, "window_start", getattr(burst, "start", None))) for burst in log_bursts]
    starts: list[float] = sorted([value for value in raw_starts if value is not None])
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
    log_bursts: list[LogBurst],
    log_patterns: list[LogPattern],
    suppression_counts: dict[str, int],
    warnings: list[str],
) -> list[LogBurst]:
    if not log_bursts:
        return log_bursts
    if not _is_precision_profile():
        return log_bursts
    if not log_patterns:
        return log_bursts
    highest_pattern_severity = max(
        (getattr(pattern, "severity", Severity.LOW).weight() for pattern in log_patterns),
        default=Severity.LOW.weight(),
    )
    if highest_pattern_severity > Severity.LOW.weight():
        return log_bursts
    if not _is_strongly_periodic_log_bursts(log_bursts):
        return log_bursts
    suppressed = len(log_bursts)
    suppression_counts["low_signal_periodic_log_bursts"] = (
        suppression_counts.get("low_signal_periodic_log_bursts", 0) + suppressed
    )
    warnings.append(f"Quality gate suppressed {suppressed} periodic low-severity log burst(s) from RCA corroboration.")
    return []


def _apply_precision_quality_gates(
    inputs: PrecisionQualityGateInputs | None = None,
    **legacy_kwargs: Any,
) -> tuple[list[MetricAnomaly], list[ChangePoint], list[RootCauseModel], list[RankedCause], AnalysisQuality]:
    kw = legacy_kwargs
    inputs = inputs or PrecisionQualityGateInputs(
        metric_anomalies=kw.get("metric_anomalies", []),
        change_points=kw.get("change_points", []),
        root_causes=kw.get("root_causes", []),
        ranked_causes=kw.get("ranked_causes", []),
        duration_seconds=float(kw.get("duration_seconds", 0.0)),
        suppression_counts=kw.get("suppression_counts", {}),
        warnings=kw.get("warnings", []),
    )
    metric_anomalies, change_points, root_causes, ranked_causes, duration_seconds, suppression_counts, warnings = (
        inputs.metric_anomalies,
        inputs.change_points,
        inputs.root_causes,
        inputs.ranked_causes,
        inputs.duration_seconds,
        inputs.suppression_counts,
        inputs.warnings,
    )

    hours = max(float(duration_seconds) / 3600.0, 1.0 / 60.0)

    if _is_precision_profile() and metric_anomalies:
        max_density = max(0.0, float(getattr(settings, "quality_max_anomaly_density_per_metric_per_hour", 0.0)))
        if max_density > 0:
            keep_per_metric = max(1, int(math.ceil(max_density * hours)))
            by_metric: dict[str, list[MetricAnomaly]] = defaultdict(list)
            for item in metric_anomalies:
                metric_name = str(getattr(item, "metric_name", "metric")).strip() or "metric"
                by_metric[metric_name].append(item)
            filtered: list[MetricAnomaly] = []
            suppressed = 0
            for items in by_metric.values():
                if len(items) <= keep_per_metric:
                    filtered.extend(items)
                    continue
                ranked = sorted(
                    items,
                    key=lambda a: (
                        getattr(getattr(a, "severity", Severity.LOW), "weight", lambda: 0)(),
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
    if _is_precision_profile() and change_points:
        max_density_cp = max(
            0.0,
            float(getattr(settings, "quality_max_change_point_density_per_metric_per_hour", 0.0)),
        )
        if max_density_cp > 0:
            keep_per_metric_cp = max(1, int(math.ceil(max_density_cp * hours)))
            by_metric_cp: dict[str, list[ChangePoint]] = defaultdict(list)
            for change_point in change_points:
                metric_name = str(getattr(change_point, "metric_name", "metric")).strip() or "metric"
                by_metric_cp[metric_name].append(change_point)
            filtered_cp: list[ChangePoint] = []
            suppressed_cp = 0
            for change_point_items in by_metric_cp.values():
                if len(change_point_items) <= keep_per_metric_cp:
                    filtered_cp.extend(change_point_items)
                    continue
                ranked_cp = sorted(
                    change_point_items,
                    key=lambda c: (
                        float(getattr(c, "magnitude", 0.0)),
                        float(getattr(c, "timestamp", 0.0)),
                    ),
                    reverse=True,
                )
                filtered_cp.extend(ranked_cp[:keep_per_metric_cp])
                suppressed_cp += len(change_point_items) - keep_per_metric_cp
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

        if _is_precision_profile():
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

            multi_signal = [cause for cause in root_causes if _root_cause_signal_count(cause) >= min_corr]
            if not multi_signal and len(root_causes) > max_without:
                suppressed_without_multi = len(root_causes) - max_without
                root_causes = root_causes[:max_without]
                suppression_counts["root_causes_without_multisignal"] = (
                    suppression_counts.get("root_causes_without_multisignal", 0) + suppressed_without_multi
                )
                warnings.append(
                    "Quality gate suppressed "
                    f"{suppressed_without_multi} root cause(s) without multi-signal corroboration."
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

        for cause in root_causes:
            if not getattr(cause, "corroboration_summary", None):
                cause.corroboration_summary = _root_cause_corroboration_summary(cause)
            diagnostics = dict(getattr(cause, "suppression_diagnostics", {}) or {})
            diagnostics.setdefault(
                "gating_profile",
                str(getattr(settings, "quality_gating_profile", "precision_strict_v1")).strip()
                or "precision_strict_v1",
            )
            signal_count = _root_cause_signal_count(cause)
            diagnostics.setdefault("signal_count", signal_count)
            diagnostics["min_corroboration_signals"] = min_corr
            diagnostics["meets_min_corroboration_signals"] = signal_count >= min_corr
            cause.suppression_diagnostics = diagnostics

    quality = AnalysisQuality(
        anomaly_density=_compute_anomaly_density(metric_anomalies, duration_seconds),
        suppression_counts={k: int(v) for k, v in suppression_counts.items() if int(v) > 0},
        gating_profile=str(getattr(settings, "quality_gating_profile", "precision_strict_v1")).strip()
        or "precision_strict_v1",
        confidence_calibration_version=str(
            getattr(settings, "quality_confidence_calibration_version", "calib_2026_02_25")
        ),
    )
    return metric_anomalies, change_points, root_causes, ranked_causes, quality


async def _process_one_metric_series(
    **legacy_kwargs: Any,
) -> tuple[list[MetricAnomaly], list[ChangePoint], TrajectoryForecast | None, DegradationSignal | None]:
    job = MetricSeriesJob(
        req=legacy_kwargs["req"],
        query_string=legacy_kwargs["query_string"],
        metric_name=legacy_kwargs["metric_name"],
        ts=legacy_kwargs["ts"],
        vals=legacy_kwargs["vals"],
        z_threshold=legacy_kwargs["z_threshold"],
        analysis_window_seconds=legacy_kwargs["analysis_window_seconds"],
    )
    req, metric_name, ts, vals, z_threshold = job.req, job.metric_name, job.ts, job.vals, job.z_threshold
    try:
        # result is persisted by store; value not used later
        _ = await baseline_store.compute_and_persist(req.tenant_id, metric_name, ts, vals, z_threshold)
    except _RECOVERABLE_ANALYSIS_ERRORS:
        # fallback compute also only triggers side‑effects
        _ = baseline_compute(ts, vals, z_threshold=z_threshold)

    metric_anomalies = anomaly.detect(metric_name, ts, vals, req.sensitivity)
    sigma_multiplier = (
        float(z_threshold)
        if z_threshold and math.isfinite(float(z_threshold))
        else float(settings.cusum_threshold_sigma)
    )
    sigma_multiplier = max(1.0, sigma_multiplier)
    try:
        change_points = changepoint_detect(
            job.ts,
            job.vals,
            threshold_sigma=sigma_multiplier,
            metric_name=metric_name,
        )
    except TypeError:
        # Backward-compatible path for monkeypatched/legacy detector signatures.
        change_points = changepoint_detect(job.ts, job.vals, sigma_multiplier)

    threshold = next((v for k, v in FORECAST_THRESHOLDS.items() if k in job.query_string), None)
    if threshold and job.analysis_window_seconds >= float(
        getattr(settings, "analyzer_forecast_min_window_seconds", 0.0)
    ):
        fc = forecast(metric_name, ts, vals, threshold, req.forecast_horizon_seconds)
    else:
        fc = None

    if job.analysis_window_seconds >= float(getattr(settings, "analyzer_degradation_min_window_seconds", 0.0)):
        deg = analyze_degradation(metric_name, ts, vals)
    else:
        deg = None

    return metric_anomalies, change_points, fc, deg


async def _process_metrics(
    provider: DataSourceProvider,
    req: AnalyzeRequest,
    all_metric_queries: list[str],
    z_threshold: float,
    analysis_window_seconds: float,
) -> tuple[
    list[MetricAnomaly],
    list[ChangePoint],
    list[TrajectoryForecast],
    list[DegradationSignal],
    dict[str, list[float]],
    list[MetricSeriesDistributionStats],
]:
    metrics_raw = await fetch_metrics(provider, all_metric_queries, req.start, req.end, req.step)
    requested_services = _normalize_services(req.services)
    if requested_services:
        filtered_metrics_raw: list[tuple[str, JSONDict]] = []
        for query_string, resp in metrics_raw:
            filtered_resp = _filter_metric_response_by_services(resp, requested_services)
            filtered_metrics_raw.append((query_string, filtered_resp if isinstance(filtered_resp, dict) else {}))
        metrics_raw = filtered_metrics_raw

    series_list: list[tuple[str, str, list[float], list[float]]] = [
        (query_string, metric_name, ts, vals)
        for query_string, resp in metrics_raw
        for metric_name, ts, vals in anomaly.iter_series(resp, query_hint=query_string)
    ]

    distribution_by_key: dict[str, MetricSeriesDistributionStats] = {}
    for query_string, metric_name, _ts, vals in series_list:
        sk = _series_key(query_string, metric_name)
        row = compute_series_distribution_stats(sk, metric_name, vals)
        if row is not None:
            distribution_by_key[sk] = row
    distribution_stats = list(distribution_by_key.values())

    shared_kwargs = {"req": req, "z_threshold": z_threshold, "analysis_window_seconds": analysis_window_seconds}
    tasks = [
        _process_one_metric_series(**shared_kwargs, query_string=q, metric_name=m, ts=t, vals=v)
        for q, m, t, v in series_list
    ]
    processed = await asyncio.gather(*tasks, return_exceptions=True)

    metric_anomalies: list[MetricAnomaly] = []
    change_points: list[ChangePoint] = []
    forecasts: list[TrajectoryForecast] = []
    degradation_signals: list[DegradationSignal] = []
    series_map: dict[str, list[float]] = {}

    for (query_string, metric_name, _ts, vals), result in zip(series_list, processed):
        series_map[_series_key(query_string, metric_name)] = vals
        if isinstance(result, BaseException):
            log.warning("Metric stage failed for %s (%s): %s", metric_name, query_string, result)
            continue
        metric_stage_anomalies, metric_stage_changes, fc, deg = result
        metric_anomalies.extend(metric_stage_anomalies)
        change_points.extend(metric_stage_changes)
        if fc:
            forecasts.append(fc)
        if deg:
            degradation_signals.append(deg)

    return metric_anomalies, change_points, forecasts, degradation_signals, series_map, distribution_stats


def _slo_series_pairs(
    err_raw: anomaly.series.WrappedMimirResponse,
    tot_raw: anomaly.series.WrappedMimirResponse,
    warnings: list[str],
) -> list[tuple[list[float], list[float], list[float]]]:
    return _slo_series_pairs_impl(
        err_raw,
        tot_raw,
        warnings,
        error_query=SLO_ERROR_QUERY,
        total_query=SLO_TOTAL_QUERY,
    )


def _select_granger_series(series_map: dict[str, list[float]]) -> dict[str, list[float]]:
    return _select_granger_series_impl(series_map)
