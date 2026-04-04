"""
Test Anomaly Series Detection and correlation logic in the analysis engine, ensuring correct identification of anomalies
and their relationships.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
"""

from engine.anomaly.series import iter_series


def _resp(metric: dict, values: list[list[object]]) -> dict:
    return {
        "status": "success",
        "data": {
            "result": [
                {
                    "metric": metric,
                    "values": values,
                }
            ]
        },
    }


def test_iter_series_uses_query_hint_when_metric_name_is_generic():
    raw = _resp({"__name__": "metric"}, [[1, "1.0"], [2, "2.0"]])
    rows = list(iter_series(raw, query_hint="system_memory_usage_bytes"))
    assert rows
    metric_name, ts, vals = rows[0]
    assert metric_name.startswith("system_memory_usage_bytes")
    assert ts == [1.0, 2.0]
    assert vals == [1.0, 2.0]


def test_iter_series_extracts_hint_from_tuple_shape():
    raw = ("sum(rate(system_filesystem_usage_bytes[5m]))", _resp({}, [[1, "0.8"], [2, "0.9"]]))
    rows = list(iter_series(raw))
    assert rows
    assert rows[0][0].startswith("system_filesystem_usage_bytes")


def test_iter_series_preserves_non_generic_metric_name():
    raw = _resp({"__name__": "system_cpu_time_seconds_total", "service": "api"}, [[1, "3"], [2, "4"]])
    rows = list(iter_series(raw, query_hint="sum(rate(other_metric[5m]))"))
    assert rows
    assert rows[0][0].startswith("system_cpu_time_seconds_total")


def test_iter_series_skips_malformed_points_without_nan_padding():
    raw = _resp({"__name__": "metric"}, [[1, "1.0"], ["bad", "x"], [2, "2.0"]])
    rows = list(iter_series(raw, query_hint="system_memory_usage_bytes"))
    assert rows
    metric_name, ts, vals = rows[0]
    assert metric_name.startswith("system_memory_usage_bytes")
    assert ts == [1.0, 2.0]
    assert vals == [1.0, 2.0]


def test_iter_series_uses_label_based_fallback_when_no_metric_name_or_hint():
    raw = _resp({"service": "checkout", "pod": "checkout-0"}, [[1, "1.0"], [2, "1.5"]])
    rows = list(iter_series(raw))
    assert rows
    assert rows[0][0].startswith("series_service")
    assert "service=checkout" in rows[0][0]


def test_iter_series_includes_process_labels_when_present():
    raw = _resp(
        {
            "__name__": "process_cpu_time_seconds_total",
            "service_name": "cache",
            "process_executable_name": "redis-server",
            "process_pid": "274",
        },
        [[1, "1.0"], [2, "1.5"]],
    )
    rows = list(iter_series(raw))
    assert rows
    name = rows[0][0]
    assert "process_executable_name=redis-server" in name
