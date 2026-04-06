"""
RCA hypothesis generation based on correlated events, error propagation analysis, and multi-signal correlation patterns,
with confidence scoring and severity categorization.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

from pydantic import ConfigDict

from api.responses import (
    ErrorPropagation,
    LogBurst,
    LogPattern,
    MetricAnomaly,
    ServiceLatency,
)
from config import settings
from engine.correlation.temporal import CorrelatedEvent
from engine.enums import RcaCategory, Severity
from engine.events.registry import DeploymentEvent, EventRegistry
from engine.rca.scoring import (
    categorize,
    score_correlated_event,
    score_deployment_correlation,
    score_error_propagation,
)
from engine.topology.graph import DependencyGraph

_METRIC_LABEL_RE = re.compile(r"\{([^}]*)\}")
_PROCESS_NAME_KEYS = (
    "process.executable.name",
    "process_executable_name",
    "process.command",
    "process_command",
    "process.command_line",
    "process_command_line",
    "process.name",
    "process_name",
    "process",
)
_PROCESS_PID_KEYS = ("process.pid", "process_pid", "pid")


@dataclass
class HypothesisRootCause:
    __pydantic_config__ = ConfigDict(title="HypothesisRootCause")

    hypothesis: str
    confidence: float
    severity: Severity
    category: RcaCategory
    evidence: list[str] = field(default_factory=list)
    contributing_signals: list[str] = field(default_factory=list)
    affected_services: list[str] = field(default_factory=list)
    recommended_action: str = ""
    deployment: DeploymentEvent | None = None
    corroboration_summary: str = ""


# Backward-compatible alias for existing imports.
RootCause = HypothesisRootCause


@dataclass(frozen=True)
class RcaSignalInputs:
    metric_anomalies: list[MetricAnomaly] = field(default_factory=list)
    log_bursts: list[LogBurst] = field(default_factory=list)
    log_patterns: list[LogPattern] = field(default_factory=list)
    service_latency: list[ServiceLatency] = field(default_factory=list)
    error_propagation: list[ErrorPropagation] = field(default_factory=list)


def _coerce_signal_inputs(
    signal_inputs: RcaSignalInputs | list[MetricAnomaly] | None,
    legacy_signal_groups: tuple[object, ...],
) -> RcaSignalInputs:
    if isinstance(signal_inputs, RcaSignalInputs):
        return signal_inputs

    groups: list[object] = []
    if signal_inputs is not None:
        groups.append(signal_inputs)
    groups.extend(legacy_signal_groups)

    def _as_list(value: object) -> list[object]:
        return value if isinstance(value, list) else []

    if not groups:
        return RcaSignalInputs()

    metric_anomalies = _as_list(groups[0])
    log_bursts = _as_list(groups[1]) if len(groups) > 1 else []
    log_patterns = _as_list(groups[2]) if len(groups) > 2 else []
    service_latency = _as_list(groups[3]) if len(groups) > 3 else []
    error_propagation = _as_list(groups[4]) if len(groups) > 4 else []

    return RcaSignalInputs(
        metric_anomalies=[item for item in metric_anomalies if isinstance(item, MetricAnomaly)],
        log_bursts=[item for item in log_bursts if isinstance(item, LogBurst)],
        log_patterns=[item for item in log_patterns if isinstance(item, LogPattern)],
        service_latency=[item for item in service_latency if isinstance(item, ServiceLatency)],
        error_propagation=[item for item in error_propagation if isinstance(item, ErrorPropagation)],
    )


def _anomaly_impact_rank(anomaly: MetricAnomaly) -> tuple[float, float, float]:
    """
    Higher tuple = more important for narrative selection (matches report intent).
    """
    sev = getattr(anomaly, "severity", None)
    try:
        weight = float(sev.weight()) if sev is not None else 0.0
    except (AttributeError, TypeError):
        weight = 0.0
    z = abs(float(getattr(anomaly, "z_score", 0.0)))
    mad = abs(float(getattr(anomaly, "mad_score", 0.0)))
    return (weight, z, mad)


def _metric_names_for_hypothesis(metric_anomalies: list[MetricAnomaly], limit: int = 2) -> list[str]:
    """
    Pick metric names to cite in the hypothesis from a correlated event.

    We keep the strongest anomaly per metric_name, then rank names by severity / |z| / |MAD|. Alphabetical order was
    misleading (e.g. pid=160 before pid=520145 regardless of impact).
    """
    best_by_name: dict[str, MetricAnomaly] = {}
    for anomaly in metric_anomalies:
        name = str(getattr(anomaly, "metric_name", "") or "").strip()
        if not name:
            continue
        prev = best_by_name.get(name)
        if prev is None or _anomaly_impact_rank(anomaly) > _anomaly_impact_rank(prev):
            best_by_name[name] = anomaly
    ordered = sorted(
        best_by_name.keys(),
        key=lambda n: (_anomaly_impact_rank(best_by_name[n]), n),
        reverse=True,
    )
    return ordered[:limit]


def _process_entities_for_hypothesis(metric_anomalies: list[MetricAnomaly], limit: int = 2) -> list[str]:
    """
    Top process hotspots by anomaly strength, not lexicographic entity string.
    """
    best_by_entity: dict[str, tuple[tuple[float, float, float], MetricAnomaly]] = {}
    for anomaly in metric_anomalies:
        entity = _process_entity_from_metric_name(getattr(anomaly, "metric_name", ""))
        if not entity:
            continue
        rank = _anomaly_impact_rank(anomaly)
        prev = best_by_entity.get(entity)
        if prev is None or rank > prev[0]:
            best_by_entity[entity] = (rank, anomaly)
    ordered = sorted(
        best_by_entity.keys(),
        key=lambda e: best_by_entity[e][0],
        reverse=True,
    )
    return ordered[:limit]


def _evidence_score(entries: list[str]) -> float:
    total = 0.0
    for entry in entries:
        text = str(entry)
        if "=" not in text:
            continue
        try:
            total += float(text.split("=", 1)[1])
        except (TypeError, ValueError, IndexError):
            continue
    return total


def _dedupe_causes(causes: list[RootCause]) -> list[RootCause]:
    selected: dict[tuple[str, str], RootCause] = {}
    for cause in causes:
        key = (str(cause.category.value), str(cause.hypothesis))
        current = selected.get(key)
        if current is None:
            selected[key] = cause
            continue
        if cause.confidence > current.confidence:
            winner, loser = cause, current
        elif cause.confidence < current.confidence:
            winner, loser = current, cause
        else:
            winner, loser = (
                (cause, current)
                if _evidence_score(cause.evidence) >= _evidence_score(current.evidence)
                else (current, cause)
            )
        winner.affected_services = list(
            dict.fromkeys((winner.affected_services or []) + (loser.affected_services or []))
        )
        selected[key] = winner
    return list(selected.values())


def _signals_from_event(event: CorrelatedEvent) -> list[str]:
    signals: list[str] = []
    metric_names = list(dict.fromkeys(a.metric_name for a in event.metric_anomalies if a.metric_name))
    if metric_names:
        signals.append("metrics")
        signals.extend([f"metric:{name}" for name in metric_names[:3]])
    if event.log_bursts:
        signals.append("logs")
        signals.append("log:bursts")
    latency_services = list(dict.fromkeys(s.service for s in event.service_latency if getattr(s, "service", None)))
    if latency_services:
        signals.append("traces")
        signals.extend([f"trace:{service}" for service in latency_services[:2]])
    if not signals:
        return ["metrics"]
    return list(dict.fromkeys(signals))


def _extract_metric_labels(metric_name: str) -> dict[str, str]:
    text = str(metric_name or "")
    match = _METRIC_LABEL_RE.search(text)
    if not match:
        return {}
    raw_labels = match.group(1).strip()
    if not raw_labels:
        return {}
    labels: dict[str, str] = {}
    for token in raw_labels.split(","):
        if "=" not in token:
            continue
        key, value = token.split("=", 1)
        k = str(key).strip()
        v = str(value).strip().strip('"').strip("'")
        if k and v:
            labels[k] = v
    return labels


def _process_entity_from_metric_name(metric_name: str) -> str:
    labels = _extract_metric_labels(metric_name)
    process_name = ""
    for key in _PROCESS_NAME_KEYS:
        value = labels.get(key)
        if value:
            process_name = str(value).strip()
            break
    if not process_name:
        return ""
    process_pid = ""
    for key in _PROCESS_PID_KEYS:
        value = labels.get(key)
        if value:
            process_pid = str(value).strip()
            break
    if process_pid:
        return f"{process_name}(pid={process_pid})"
    return process_name


def _corroboration_summary(signals: list[str]) -> str:
    roots = []
    for signal in signals:
        text = str(signal or "").strip().lower()
        if not text:
            continue
        if ":" in text:
            text = text.split(":", 1)[0]
        if text in {"metric", "metrics"}:
            roots.append("metrics")
        elif text in {"log", "logs"}:
            roots.append("logs")
        elif text in {"trace", "traces"}:
            roots.append("traces")
        elif text in {"event", "events", "deployment", "deploy"}:
            roots.append("events")
    unique = sorted(set(roots))
    if not unique:
        return "single-signal evidence"
    return f"{len(unique)} corroborating signal(s): {', '.join(unique)}"


def _action_for_category(category: RcaCategory | None, service: str = "") -> str:
    if category is None:
        return "Investigate correlated signals."
    actions = {
        RcaCategory.DEPLOYMENT: f"Rollback recent deployment for {service or 'affected service'}.",
        RcaCategory.RESOURCE_EXHAUSTION: "Check resource limits, scale horizontally or increase quotas.",
        RcaCategory.DEPENDENCY_FAILURE: "Inspect downstream dependencies and circuit breakers.",
        RcaCategory.TRAFFIC_SURGE: "Verify rate limits, auto-scaling triggers, and CDN caching.",
        RcaCategory.ERROR_PROPAGATION: f"Isolate {service or 'source service'} and check recent changes.",
        RcaCategory.SLO_BURN: "Immediate incident response; error budget critical.",
        RcaCategory.UNKNOWN: "Review correlated signals and recent changes.",
    }
    return actions.get(category, "Investigate correlated signals.")


def generate(
    signal_inputs: RcaSignalInputs | list[MetricAnomaly] | None,
    *legacy_signal_groups: object,
    correlated_events: list[CorrelatedEvent] | None = None,
    graph: DependencyGraph | None = None,
    event_registry: EventRegistry | None = None,
) -> list[RootCause]:
    inputs = _coerce_signal_inputs(signal_inputs, legacy_signal_groups)
    _ = (inputs.metric_anomalies, inputs.log_bursts, inputs.service_latency)
    causes: list[RootCause] = []
    deployments = event_registry.list_all() if event_registry else []

    for event in correlated_events or []:
        if event.confidence < settings.rca_event_confidence_threshold:
            continue
        event_window_start = event.window_start

        category = categorize(event, deployments)
        base_score = score_correlated_event(event)
        deploy_score = score_deployment_correlation(event.window_start, deployments)
        confidence = round(min(settings.rca_score_cap, base_score + deploy_score * 0.2), 3)

        deploy_event: DeploymentEvent | None = None
        window_seconds = float(settings.rca_deploy_window_seconds)
        window_start = float(event.window_start) - window_seconds
        window_end = float(event.window_start) + window_seconds

        def _deployment_distance(
            deployment: DeploymentEvent,
            reference_time: float = event_window_start,
        ) -> float:
            return abs(deployment.timestamp - reference_time)

        if event_registry:
            nearby_deploys = event_registry.in_window(window_start, window_end)
        else:
            nearby_deploys = [d for d in deployments if window_start <= d.timestamp <= window_end]
        if nearby_deploys:
            deploy_event = min(nearby_deploys, key=_deployment_distance)

        affected: list[str] = []
        root_svc = ""
        if event.service_latency and graph:
            root_svc = event.service_latency[0].service
            blast = graph.blast_radius(root_svc)
            affected = blast.affected_downstream
            if event_registry:
                service_deploys = [
                    d for d in event_registry.for_service(root_svc) if window_start <= d.timestamp <= window_end
                ]
                if service_deploys:
                    deploy_event = min(service_deploys, key=_deployment_distance)

        metric_names = _metric_names_for_hypothesis(event.metric_anomalies, limit=2)
        svc_names = sorted({s.service for s in event.service_latency})[:2]
        process_entities = _process_entities_for_hypothesis(event.metric_anomalies, limit=2)

        parts = []
        if deploy_event:
            parts.append(f"deployment of {deploy_event.service} v{deploy_event.version}")
        if metric_names:
            parts.append(f"metric anomaly in {', '.join(metric_names)}")
        if process_entities:
            parts.append(f"process hotspot in {', '.join(process_entities)}")
        if svc_names:
            parts.append(f"latency spike in {', '.join(svc_names)}")
        if event.log_bursts:
            parts.append(f"{len(event.log_bursts)} log burst(s)")

        hypothesis = f"[{category.value}] Correlated incident: {' + '.join(parts) or 'multi-signal event'}"

        event_signals = _signals_from_event(event)
        causes.append(
            RootCause(
                hypothesis=hypothesis,
                confidence=confidence,
                severity=Severity.from_score(confidence),
                category=category,
                evidence=[
                    f"metrics={len(event.metric_anomalies)}",
                    f"process_entities={len(process_entities)}",
                    f"log_bursts={len(event.log_bursts)}",
                    f"latency_services={len(event.service_latency)}",
                ],
                contributing_signals=event_signals,
                affected_services=affected,
                recommended_action=_action_for_category(category, root_svc),
                deployment=deploy_event,
                corroboration_summary=_corroboration_summary(event_signals),
            )
        )

    for prop in inputs.error_propagation:
        svc = prop.source_service
        affected = getattr(prop, "affected_services", [])
        conf = score_error_propagation([prop])
        upstream = graph.find_upstream_roots(svc) if graph else []
        all_affected = list(dict.fromkeys(upstream + affected))
        causes.append(
            RootCause(
                hypothesis=f"[error_propagation] Errors originating from {svc}, cascading to {', '.join(affected[:3])}",
                confidence=conf,
                severity=Severity.HIGH,
                category=RcaCategory.ERROR_PROPAGATION,
                contributing_signals=[f"trace:propagation:{svc}"],
                affected_services=all_affected,
                recommended_action=_action_for_category(RcaCategory.ERROR_PROPAGATION, svc),
                corroboration_summary=_corroboration_summary([f"trace:propagation:{svc}"]),
            )
        )

    critical_patterns = [
        p for p in inputs.log_patterns if p.severity.weight() >= settings.rca_severity_weight_threshold
    ]
    if critical_patterns:
        causes.append(
            RootCause(
                hypothesis=(
                    f"[log_pattern] {len(critical_patterns)} critical pattern(s): {critical_patterns[0].pattern[:80]}"
                ),
                confidence=settings.rca_log_pattern_score,
                severity=Severity.HIGH,
                category=RcaCategory.UNKNOWN,
                contributing_signals=[f"log:{p.pattern[:40]}" for p in critical_patterns[:3]],
                recommended_action="Review high-severity log patterns for error root cause.",
                corroboration_summary=_corroboration_summary([f"log:{p.pattern[:40]}" for p in critical_patterns[:3]]),
            )
        )

    causes = _dedupe_causes(causes)
    causes.sort(key=lambda c: c.confidence, reverse=True)
    min_conf = float(settings.rca_min_confidence_display)
    filtered = [cause for cause in causes if cause.confidence >= min_conf]
    if filtered:
        return filtered
    if causes:
        top = causes[0]
        top.hypothesis = f"[low_confidence] {top.hypothesis}"
        return [top]
    return causes
