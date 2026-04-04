"""
Test cases for trace analysis logic in the analysis engine, including detection of trace anomalies, correlation with
metrics and logs, and edge cases in timestamp handling and service topology.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
"""

from engine.traces.errors import detect_propagation
from engine.traces.latency import analyze


def _trace(service: str, duration_ms: float, status_code: str, start_s: float, peer_service: str | None = None) -> dict:
    attrs = [
        {"key": "status.code", "value": {"stringValue": status_code}},
        {"key": "service.name", "value": {"stringValue": service}},
    ]
    if peer_service:
        attrs.append({"key": "peer.service", "value": {"stringValue": peer_service}})
    return {
        "rootServiceName": service,
        "rootTraceName": f"{service}.op",
        "durationMs": duration_ms,
        "startTimeUnixNano": int(start_s * 1_000_000_000),
        "endTimeUnixNano": int((start_s + (duration_ms / 1000.0)) * 1_000_000_000),
        "spanSets": [{"spans": [{"attributes": attrs}]}],
    }


def test_latency_analyze_reads_errors_from_span_sets_shape():
    raw = {
        "traces": [
            _trace("checkout", 6000.0, "STATUS_CODE_ERROR", 100.0, peer_service="payments"),
            _trace("checkout", 6200.0, "STATUS_CODE_ERROR", 110.0, peer_service="payments"),
            _trace("checkout", 5800.0, "STATUS_CODE_ERROR", 120.0, peer_service="payments"),
        ]
    }
    rows = analyze(raw, apdex_t_ms=500.0)
    assert rows
    assert rows[0].service == "checkout"
    assert rows[0].error_rate > 0
    assert rows[0].window_start is not None
    assert rows[0].window_end is not None
    assert rows[0].window_start < rows[0].window_end


def test_error_propagation_reads_span_sets_shape():
    raw = {
        "traces": [
            _trace("payments", 1200.0, "STATUS_CODE_ERROR", 100.0, peer_service="checkout"),
            _trace("payments", 1100.0, "STATUS_CODE_ERROR", 101.0, peer_service="checkout"),
            _trace("payments", 900.0, "STATUS_CODE_ERROR", 102.0, peer_service="checkout"),
            _trace("checkout", 700.0, "STATUS_CODE_ERROR", 103.0, peer_service="db"),
        ]
    }
    rows = detect_propagation(raw)
    assert rows
    assert rows[0].source_service == "payments"
    assert "checkout" in rows[0].affected_services
