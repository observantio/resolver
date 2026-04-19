"""
Test cases for log analysis logic in the analysis engine, including detection of log bursts, correlation with anomalies
and latency, and edge cases in timestamp handling and severity assignment.

Copyright (c) 2026 Stefan Kumarasinghe.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. See http://www.apache.org/licenses/LICENSE-2.0 for details.
"""

from engine.enums import Severity
from engine.logs.frequency import detect_bursts
from engine.logs.patterns import analyze


def make_loki_response(lines):
    return {"data": {"result": [{"values": [[str(int(t * 1e9)), msg] for t, msg in lines]}]}}


def test_detect_bursts():
    lines = [(i, "msg") for i in range(20)]
    resp = make_loki_response(lines)
    bursts = detect_bursts(resp, window_seconds=5)
    assert isinstance(bursts, list)

    lines = [(i / 10, "msg") for i in range(10)]
    resp = make_loki_response(lines)
    bursts = detect_bursts(resp, window_seconds=1)
    assert isinstance(bursts, list)
    if bursts:
        assert bursts[0].severity.weight() >= Severity.HIGH.weight()


def test_analyze_patterns():
    lines = [(0, "error occurred"), (1, "error occurred"), (2, "ok now")]
    resp = make_loki_response(lines)
    pats = analyze(resp)
    assert pats
    assert any("error" in p.pattern for p in pats)


def test_detect_bursts_downgrades_periodic_benign_windows():
    lines = []
    for minute in range(6):
        base = float(minute * 60)
        lines.extend(
            [
                (base + 0.00, "10000 changes in 60 seconds. Saving..."),
                (base + 0.01, "Background saving started by pid 100"),
                (base + 0.02, "DB saved on disk"),
                (base + 0.03, "Fork CoW for RDB: current 1 MB"),
                (base + 0.04, "Background saving terminated with success"),
            ]
        )
    resp = make_loki_response(lines)
    bursts = detect_bursts(resp, window_seconds=10)
    assert bursts
    assert all(b.severity == Severity.LOW for b in bursts)


def test_detect_bursts_keeps_severity_for_error_windows():
    lines = []
    for minute in range(4):
        base = float(minute * 60)
        lines.extend(
            [
                (base + 0.00, "ERROR timeout while calling database"),
                (base + 0.01, "ERROR dependency unavailable"),
                (base + 0.02, "ERROR request failed"),
                (base + 0.03, "ERROR retry failed"),
                (base + 0.04, "ERROR connection refused"),
            ]
        )
    resp = make_loki_response(lines)
    bursts = detect_bursts(resp, window_seconds=10)
    assert bursts
    assert any(b.severity.weight() >= Severity.MEDIUM.weight() for b in bursts)
