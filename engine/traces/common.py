"""
Shared helpers for iterating Tempo trace spans.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
"""

from __future__ import annotations

from collections.abc import Iterable

from custom_types.json import JSONDict


def iter_trace_spans(trace: JSONDict) -> Iterable[JSONDict]:
    span_sets: list[JSONDict] = []
    span_set = trace.get("spanSet")
    if isinstance(span_set, dict):
        span_sets.append(span_set)
    span_sets_value = trace.get("spanSets")
    if isinstance(span_sets_value, list):
        span_sets.extend([span_set for span_set in span_sets_value if isinstance(span_set, dict)])
    for span_set in span_sets:
        spans = span_set.get("spans")
        if not isinstance(spans, list):
            continue
        for span in spans:
            if isinstance(span, dict):
                yield span


def span_has_error(span: JSONDict) -> bool:
    attributes = span.get("attributes")
    if not isinstance(attributes, list):
        return False
    attrs = {attr.get("key", ""): attr.get("value", {}) for attr in attributes if isinstance(attr, dict)}
    status_value = attrs.get("status.code")
    if not isinstance(status_value, dict):
        return False
    string_value = status_value.get("stringValue")
    status_code = string_value.upper() if isinstance(string_value, str) else ""
    return status_code in {"STATUS_CODE_ERROR", "ERROR"}
