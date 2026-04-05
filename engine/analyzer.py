"""
Analyzer Module for Root Cause Analysis and Correlation of Anomalies.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
"""

from __future__ import annotations

import asyncio
import dataclasses
import logging
import re
import time
from collections.abc import Sequence

import httpx

from api.requests import AnalyzeRequest
from api.responses import (
    AnalysisQuality,
    AnalysisReport,
    ErrorPropagation,
    LogBurst,
    LogPattern,
    MetricAnomaly,
    MetricSeriesDistributionStats,
    RootCause as RootCauseModel,
    ServiceLatency,
    SloBurnAlert as SloBurnAlertModel,
)
from config import DEFAULT_METRIC_QUERIES, SLO_ERROR_QUERY, SLO_TOTAL_QUERY, settings
from datasources.provider import DataSourceProvider
from engine import anomaly, logs, rca, traces
from engine.anomaly.series import WrappedMimirResponse
from engine.analyze.helpers import (
    _apply_precision_quality_gates,
    _build_compat_registry,
    _build_selection_score_components,
    _dedupe_by_metric_with_severity,
    _dedupe_change_points,
    _dedupe_metric_anomalies,
    _filter_log_bursts_for_precision_rca,
    _limit_analyzer_output,
    _process_metrics,
    _select_granger_series,
    _slo_series_pairs,
    _to_root_cause_model,
)
from engine.analyze.filters import filter_metric_response_by_services as _filter_metric_response_by_services
from engine.analyze.filters import normalize_services as _normalize_services
from engine.changepoint import ChangePoint, detect as changepoint_detect
from engine.causal import BayesianScore, CausalGraph, GrangerResult, bayesian_score, test_all_pairs
from engine.correlation import CorrelatedEvent, LogMetricLink, correlate, link_logs_to_metrics
from engine.dedup import group_metric_anomalies
from engine.enums import Severity
from engine.forecast.degradation import DegradationSignal
from engine.forecast.trajectory import TrajectoryForecast
from engine.log_query import build_log_query
from engine.ml import AnomalyCluster, RankedCause, cluster, rank
from engine.registry import TenantRegistry, TenantState, get_registry
from engine.slo import evaluate as slo_evaluate
from engine.slo.models import SloBurnAlert
from engine.topology import DependencyGraph
from store import baseline as baseline_store
from store import granger as granger_store

__all__ = [
    "anomaly",
    "baseline_store",
    "changepoint_detect",
    "granger_store",
    "run",
]

log = logging.getLogger(__name__)
_TRACE_COUNT_FALLBACK_CAP = 10_000
_RECOVERABLE_ANALYSIS_ERRORS = (
    asyncio.TimeoutError,
    httpx.HTTPError,
    OSError,
    RuntimeError,
    TypeError,
    ValueError,
)


def _overall_severity(*groups: Sequence[object]) -> Severity:
    best = Severity.LOW
    for group in groups:
        for item in group:
            severity = getattr(item, "severity", None)
            if isinstance(severity, Severity) and severity.weight() > best.weight():
                best = severity
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
        critical = sum(1 for f in report.forecasts if getattr(getattr(f, "severity", None), "weight", lambda: 0)() >= 4)
        if critical:
            parts.append(f"{critical} imminent breach(es) predicted")
    if report.degradation_signals:
        parts.append(f"{len(report.degradation_signals)} degrading metric(s)")

    if not parts:
        return "No anomalies detected in the analysis window."

    top = f" Top: {report.root_causes[0].hypothesis[:120]}..." if report.root_causes else ""
    return f"[{report.overall_severity.value.upper()}] {' | '.join(parts)}.{top}"


def _build_log_query(services: list[str] | None, requested_log_query: str | None) -> str:
    return build_log_query(services, requested_log_query)


async def _fetch_parallel_observations(
    provider: DataSourceProvider,
    req: AnalyzeRequest,
    *,
    log_query: str,
    trace_filters: dict[str, str | int | float | bool],
    warnings: list[str],
) -> tuple[object, object, object, object]:
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
    return logs_raw, traces_raw, slo_errors_raw, slo_total_raw


async def _run_metrics_stage(
    provider: DataSourceProvider,
    req: AnalyzeRequest,
    all_metric_queries: list[str],
    z_threshold: float,
    analysis_window_seconds: float,
    warnings: list[str],
    suppression_counts: dict[str, int],
) -> tuple[
    list[MetricAnomaly],
    list[ChangePoint],
    list[TrajectoryForecast],
    list[DegradationSignal],
    dict[str, list[float]],
    list[MetricSeriesDistributionStats],
]:
    metrics_started = time.perf_counter()
    try:
        (
            metric_anomalies,
            change_points,
            forecasts,
            degradation_signals,
            series_map,
            metric_series_statistics,
        ) = await asyncio.wait_for(
            _process_metrics(provider, req, all_metric_queries, z_threshold, analysis_window_seconds),
            timeout=float(settings.analyzer_metrics_timeout_seconds),
        )
    except TimeoutError:
        msg = (
            f"Metrics stage timed out after {settings.analyzer_metrics_timeout_seconds}s; " "returning partial report."
        )
        warnings.append(msg)
        log.warning(msg)
        (
            metric_anomalies,
            change_points,
            forecasts,
            degradation_signals,
            series_map,
            metric_series_statistics,
        ) = ([], [], [], [], {}, [])
    except _RECOVERABLE_ANALYSIS_ERRORS as exc:
        msg = f"Metrics unavailable: {exc}"
        warnings.append(msg)
        log.warning(msg)
        (
            metric_anomalies,
            change_points,
            forecasts,
            degradation_signals,
            series_map,
            metric_series_statistics,
        ) = ([], [], [], [], {}, [])
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
    return metric_anomalies, change_points, forecasts, degradation_signals, series_map, metric_series_statistics


async def _run_logs_stage(
    provider: DataSourceProvider,
    req: AnalyzeRequest,
    *,
    log_query: str,
    logs_raw: object,
    warnings: list[str],
) -> tuple[list[LogBurst], list[LogPattern]]:
    logs_started = time.perf_counter()
    log_bursts, log_patterns = [], []
    if isinstance(logs_raw, dict):
        logs_data = logs_raw.get("data")
        log_entries = logs_data.get("result", []) if isinstance(logs_data, dict) else []
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
                except _RECOVERABLE_ANALYSIS_ERRORS as exc:
                    log.debug("Logs fallback selector failed query=%s error=%s", selector, exc)
                    continue
                fallback_data = fallback_logs.get("data") if isinstance(fallback_logs, dict) else None
                fallback_results = fallback_data.get("result") if isinstance(fallback_data, dict) else None
                if isinstance(fallback_logs, dict) and fallback_results:
                    logs_raw = fallback_logs
                    logs_data = logs_raw.get("data")
                    log_entries = logs_data.get("result", []) if isinstance(logs_data, dict) else []
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
    return log_bursts, log_patterns


async def _run_traces_stage(
    provider: DataSourceProvider,
    req: AnalyzeRequest,
    *,
    primary_service: str | None,
    trace_filters: dict[str, str | int | float | bool],
    traces_raw: object,
    warnings: list[str],
) -> tuple[list[ServiceLatency], list[ErrorPropagation], DependencyGraph]:
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
        trace_payload = traces_raw.get("traces")
        if not trace_payload:
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
            except _RECOVERABLE_ANALYSIS_ERRORS as exc:
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
    return service_latency, error_propagation, graph


def _run_slo_stage(
    req: AnalyzeRequest,
    *,
    primary_service: str | None,
    slo_errors_raw: object,
    slo_total_raw: object,
    warnings: list[str],
) -> list[SloBurnAlertModel]:
    slo_started = time.perf_counter()
    slo_alerts_raw: list[SloBurnAlert] = []
    if isinstance(slo_errors_raw, dict) and isinstance(slo_total_raw, dict):
        requested_service_set = _normalize_services(req.services)
        filtered_slo_errors_raw: WrappedMimirResponse = slo_errors_raw
        filtered_slo_total_raw: WrappedMimirResponse = slo_total_raw
        if requested_service_set:
            filtered_errors = _filter_metric_response_by_services(slo_errors_raw, requested_service_set)
            filtered_totals = _filter_metric_response_by_services(slo_total_raw, requested_service_set)
            if isinstance(filtered_errors, dict):
                filtered_slo_errors_raw = filtered_errors
            if isinstance(filtered_totals, dict):
                filtered_slo_total_raw = filtered_totals
        for err_ts, err_vals, tot_vals in _slo_series_pairs(filtered_slo_errors_raw, filtered_slo_total_raw, warnings):
            slo_alerts_raw.extend(
                slo_evaluate(primary_service or "global", err_vals, tot_vals, err_ts, req.slo_target or 0.999)
            )
    else:
        warnings.append("SLO metrics unavailable for one or both queries.")
    slo_alerts = [SloBurnAlertModel(**dataclasses.asdict(a)) for a in slo_alerts_raw]
    log.debug("analyzer stage=slo duration=%.4fs", time.perf_counter() - slo_started)
    return slo_alerts


def _normalize_ranked_root_causes(
    ranked_causes: list[RankedCause],
    warnings: list[str],
    suppression_counts: dict[str, int],
) -> tuple[list[RootCauseModel], list[RankedCause]]:
    pydantic_root_causes: list[RootCauseModel] = []
    ranked_valid: list[RankedCause] = []
    hypothesis_to_ranked: dict[str, object] = {}
    for item in ranked_causes:
        try:
            root_cause_model = _to_root_cause_model(item.root_cause)
            pydantic_root_causes.append(root_cause_model)
            ranked_valid.append(item)
            hypothesis = str(root_cause_model.hypothesis)
            current = hypothesis_to_ranked.get(hypothesis)
            if current is None or float(getattr(item, "final_score", 0.0)) > float(
                getattr(current, "final_score", 0.0)
            ):
                hypothesis_to_ranked[hypothesis] = item
        except (AttributeError, TypeError, ValueError) as exc:
            suppression_counts["invalid_root_cause_drops"] = suppression_counts.get("invalid_root_cause_drops", 0) + 1
            warnings.append(f"Dropped invalid root cause model during normalization: {exc}")
    for cause in pydantic_root_causes:
        ranked_item = hypothesis_to_ranked.get(str(cause.hypothesis))
        if ranked_item is None:
            continue
        cause.selection_score_components = _build_selection_score_components(ranked_item, cause)
    return pydantic_root_causes, ranked_valid


async def _run_correlate_cluster_stage(
    tenant_id: str,
    metric_anomalies: list[MetricAnomaly],
    log_bursts: list[LogBurst],
    rca_log_bursts: list[LogBurst],
    service_latency: list[ServiceLatency],
    req: AnalyzeRequest,
    registry: TenantRegistry,
) -> tuple[list[LogMetricLink], TenantState, list[CorrelatedEvent], list[AnomalyCluster]]:
    correlate_started = time.perf_counter()
    log_metric_links = link_logs_to_metrics(metric_anomalies, log_bursts)
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
    return log_metric_links, state, correlated_events, anomaly_clusters


async def _run_causal_rank_and_quality(
    tenant_id: str,
    primary_service: str | None,
    req: AnalyzeRequest,
    *,
    registry: TenantRegistry,
    series_map: dict[str, list[float]],
    metric_anomalies: list[MetricAnomaly],
    rca_log_bursts: list[LogBurst],
    log_patterns: list[LogPattern],
    service_latency: list[ServiceLatency],
    error_propagation: list[ErrorPropagation],
    correlated_events: list[CorrelatedEvent],
    graph: DependencyGraph,
    change_points: list[ChangePoint],
    forecasts: list[TrajectoryForecast],
    degradation_signals: list[DegradationSignal],
    anomaly_clusters: list[AnomalyCluster],
    warnings: list[str],
    suppression_counts: dict[str, int],
) -> tuple[
    list[MetricAnomaly],
    list[ChangePoint],
    list[RootCauseModel],
    list[RankedCause],
    list[AnomalyCluster],
    list[GrangerResult],
    list[TrajectoryForecast],
    list[DegradationSignal],
    AnalysisQuality,
    list[BayesianScore],
]:
    causal_started = time.perf_counter()
    series_for_granger = _select_granger_series(series_map)
    granger_started = time.perf_counter()
    fresh_granger = (
        test_all_pairs(series_for_granger, max_lag=settings.granger_max_lag) if len(series_for_granger) >= 2 else []
    )
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
    except _RECOVERABLE_ANALYSIS_ERRORS as exc:
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

    raw_deployment_events = await registry.events_in_window(tenant_id, req.start, req.end)
    deployment_events = list(raw_deployment_events) if isinstance(raw_deployment_events, list) else []
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
    pydantic_root_causes, ranked_causes = _normalize_ranked_root_causes(
        ranked_causes, warnings, suppression_counts
    )
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
    return (
        metric_anomalies,
        change_points,
        pydantic_root_causes,
        ranked_causes,
        anomaly_clusters,
        fresh_granger,
        forecasts,
        degradation_signals,
        quality,
        bayesian_scores,
    )


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
    trace_filters: dict[str, str | int | float | bool] = {"service.name": primary_service} if primary_service else {}
    all_metric_queries = list(dict.fromkeys((req.metric_queries or []) + DEFAULT_METRIC_QUERIES))

    if req.sensitivity:
        z_threshold = 1.0 + req.sensitivity * settings.analyzer_sensitivity_factor
    else:
        z_threshold = settings.baseline_zscore_threshold

    logs_raw, traces_raw, slo_errors_raw, slo_total_raw = await _fetch_parallel_observations(
        provider, req, log_query=log_query, trace_filters=trace_filters, warnings=warnings
    )

    (
        metric_anomalies,
        change_points,
        forecasts,
        degradation_signals,
        series_map,
        metric_series_statistics,
    ) = await _run_metrics_stage(
        provider,
        req,
        all_metric_queries,
        z_threshold,
        analysis_window_seconds,
        warnings,
        suppression_counts,
    )

    log_bursts, log_patterns = await _run_logs_stage(
        provider, req, log_query=log_query, logs_raw=logs_raw, warnings=warnings
    )

    service_latency, error_propagation, graph = await _run_traces_stage(
        provider,
        req,
        primary_service=primary_service,
        trace_filters=trace_filters,
        traces_raw=traces_raw,
        warnings=warnings,
    )

    slo_alerts = _run_slo_stage(
        req,
        primary_service=primary_service,
        slo_errors_raw=slo_errors_raw,
        slo_total_raw=slo_total_raw,
        warnings=warnings,
    )

    rca_log_bursts = _filter_log_bursts_for_precision_rca(
        log_bursts=log_bursts,
        log_patterns=log_patterns,
        suppression_counts=suppression_counts,
        warnings=warnings,
    )
    # Keep raw links for investigation UX; filtered bursts are used for RCA correlation/scoring only.
    log_metric_links, _, correlated_events, anomaly_clusters = await _run_correlate_cluster_stage(
        tenant_id,
        metric_anomalies,
        log_bursts,
        rca_log_bursts,
        service_latency,
        req,
        registry,
    )

    (
        metric_anomalies,
        change_points,
        pydantic_root_causes,
        ranked_causes,
        anomaly_clusters,
        fresh_granger,
        forecasts,
        degradation_signals,
        quality,
        bayesian_scores,
    ) = await _run_causal_rank_and_quality(
        tenant_id,
        primary_service,
        req,
        registry=registry,
        series_map=series_map,
        metric_anomalies=metric_anomalies,
        rca_log_bursts=rca_log_bursts,
        log_patterns=log_patterns,
        service_latency=service_latency,
        error_propagation=error_propagation,
        correlated_events=correlated_events,
        graph=graph,
        change_points=change_points,
        forecasts=forecasts,
        degradation_signals=degradation_signals,
        anomaly_clusters=anomaly_clusters,
        warnings=warnings,
        suppression_counts=suppression_counts,
    )

    severity = _overall_severity(
        metric_anomalies,
        log_bursts,
        log_patterns,
        service_latency,
        slo_alerts,
        forecasts,
    )
    has_actionable_now = bool(
        metric_anomalies or log_bursts or log_patterns or service_latency or error_propagation or slo_alerts
    )
    if not has_actionable_now and (forecasts or degradation_signals or change_points):
        if severity.weight() > Severity.MEDIUM.weight():
            warnings.append(
                "Overall severity was capped at MEDIUM because only predictive signals were present "
                "without corroborating actionable anomalies."
            )
            severity = Severity.MEDIUM

    forecasts = [item for item in forecasts if isinstance(item, TrajectoryForecast)]
    degradation_signals = [item for item in degradation_signals if isinstance(item, DegradationSignal)]

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
        metric_series_statistics=metric_series_statistics,
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
