"""Shared helpers for iterating Tempo trace spans."""

from __future__ import annotations

from typing import Any, Dict, Iterable


def iter_trace_spans(trace: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    span_sets = []
    if isinstance(trace.get("spanSet"), dict):
        span_sets.append(trace.get("spanSet"))
    if isinstance(trace.get("spanSets"), list):
        span_sets.extend([span_set for span_set in trace.get("spanSets") if isinstance(span_set, dict)])
    for span_set in span_sets:
        for span in span_set.get("spans", []):
            if isinstance(span, dict):
                yield span


def span_has_error(span: Dict[str, Any]) -> bool:
    attrs = {attr.get("key", ""): attr.get("value", {}) for attr in span.get("attributes", [])}
    status_code = attrs.get("status.code", {}).get("stringValue", "").upper()
    return status_code in {"STATUS_CODE_ERROR", "ERROR"}
