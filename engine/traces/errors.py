"""
Error propagation detection for traces, identifying source services and affected downstream services.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping

from api.responses import ErrorPropagation
from config import settings
from engine.enums import Severity
from engine.topology import DependencyGraph
from engine.traces.common import iter_trace_spans, span_has_error


def detect_propagation(tempo_response: Mapping[str, object]) -> list[ErrorPropagation]:
    service_errors: dict[str, int] = defaultdict(int)
    service_total: dict[str, int] = defaultdict(int)
    graph = DependencyGraph()
    graph.from_spans(tempo_response)

    traces = tempo_response.get("traces")
    if not isinstance(traces, list):
        return []

    for trace in traces:
        if not isinstance(trace, dict):
            continue
        service = trace.get("rootServiceName", "unknown")
        service_total[service] += 1
        has_error = False

        for span in iter_trace_spans(trace):
            if span_has_error(span):
                has_error = True
                break

        if has_error:
            service_errors[service] += 1

    error_rates = {svc: service_errors[svc] / service_total[svc] for svc in service_total if service_total[svc] > 0}

    sources = [svc for svc, rate in error_rates.items() if rate >= settings.trace_error_rate_threshold]
    if not sources:
        return []

    results: list[ErrorPropagation] = []

    for source in sources:
        affected_services = sorted(graph.blast_radius(source).affected_downstream)
        if not affected_services:
            continue

        rate = error_rates[source]
        if rate >= settings.trace_error_severity_critical:
            severity = Severity.CRITICAL
        elif rate >= settings.trace_error_severity_high:
            severity = Severity.HIGH
        else:
            severity = Severity.MEDIUM

        results.append(
            ErrorPropagation(
                source_service=source,
                affected_services=affected_services,
                error_rate=round(rate, 4),
                severity=severity,
            )
        )
    results.sort(key=lambda e: e.error_rate, reverse=True)
    return results
