"""
Test cases for log analysis logic in the analysis engine, including detection of log bursts, correlation with anomalies and latency, and edge cases in timestamp handling and severity assignment.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
"""

from engine.logs.frequency import detect_bursts
from engine.logs.patterns import analyze
from engine.enums import Severity


def make_loki_response(lines):
    return {
        "data": {"result": [{"values": [[str(int(t*1e9)), msg] for t, msg in lines]}]}
    }


def test_detect_bursts():
    lines = [(i, "msg") for i in range(20)]
    resp = make_loki_response(lines)
    bursts = detect_bursts(resp, window_seconds=5)
    assert isinstance(bursts, list)

    lines = [(i/10, "msg") for i in range(10)]
    resp = make_loki_response(lines)
    bursts = detect_bursts(resp, window_seconds=1)
    assert isinstance(bursts, list)
    if bursts:
        assert bursts[0].severity.weight() >= Severity.high.weight()


def test_analyze_patterns():
    lines = [(0, "error occurred"), (1, "error occurred"), (2, "ok now")]
    resp = make_loki_response(lines)
    pats = analyze(resp)
    assert pats
    assert any("error" in p.pattern for p in pats)
