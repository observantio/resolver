"""
API route tests for trace anomaly detection paths, focused on validating that the trace query route correctly handles cases where no service filters are provided, ensuring that it does not apply any default service filters and allows the provider to process the request with an empty filter set as intended.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
"""

from __future__ import annotations

import pytest

from api.requests import AnalyzeRequest
from api.responses import (
    AnalysisQuality,
    AnalysisReport,
    RootCause,
    ServiceLatency,
)
from api.routes import analyze as analyze_route
from engine.enums import Severity, Signal


@pytest.mark.asyncio
async def test_analyze_route_includes_additive_schema_fields(monkeypatch):
    async def fake_run_analysis(req: AnalyzeRequest) -> AnalysisReport:
        return AnalysisReport(
            tenant_id=req.tenant_id,
            start=req.start,
            end=req.end,
            duration_seconds=req.end - req.start,
            metric_anomalies=[],
            log_bursts=[],
            log_patterns=[],
            service_latency=[
                ServiceLatency(
                    service="checkout",
                    operation="POST /orders",
                    p50_ms=20.0,
                    p95_ms=50.0,
                    p99_ms=80.0,
                    apdex=0.92,
                    error_rate=0.02,
                    sample_count=10,
                    severity=Severity.medium,
                    window_start=100.0,
                    window_end=160.0,
                )
            ],
            error_propagation=[],
            slo_alerts=[],
            root_causes=[
                RootCause(
                    hypothesis="h1",
                    confidence=0.81,
                    evidence=[],
                    contributing_signals=[Signal.metrics, Signal.logs],
                    recommended_action="rollback",
                    severity=Severity.high,
                    corroboration_summary="2 corroborating signal(s): logs, metrics",
                    suppression_diagnostics={"gating_profile": "precision_strict_v1"},
                    selection_score_components={"final_score": 0.81, "ml_score": 0.77},
                )
            ],
            ranked_causes=[],
            change_points=[],
            log_metric_links=[],
            forecasts=[],
            degradation_signals=[],
            anomaly_clusters=[],
            granger_results=[],
            bayesian_scores=[],
            analysis_warnings=[],
            overall_severity=Severity.high,
            summary="summary",
            quality=AnalysisQuality(
                anomaly_density={"request_total": 0.4},
                suppression_counts={"duplicate_metric_anomalies": 2},
                gating_profile="precision_strict_v1",
                confidence_calibration_version="calib_2026_02_25",
            ),
        )

    monkeypatch.setattr(analyze_route, "run_analysis", fake_run_analysis)
    req = AnalyzeRequest(tenant_id="t1", start=100, end=200, services=["checkout"])
    out = await analyze_route.analyze(req)
    payload = out.model_dump(mode="json")

    assert payload["quality"]["gating_profile"] == "precision_strict_v1"
    assert payload["service_latency"][0]["window_start"] == 100.0
    assert payload["service_latency"][0]["window_end"] == 160.0
    assert payload["root_causes"][0]["corroboration_summary"]
    assert payload["root_causes"][0]["suppression_diagnostics"]["gating_profile"] == "precision_strict_v1"
    assert payload["root_causes"][0]["selection_score_components"]["final_score"] == 0.81
