"""
Tests for per-job RCA YAML configuration overrides.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
"""

from __future__ import annotations

import copy
from typing import Any, cast

import pytest
from fastapi import HTTPException

from api.requests import AnalyzeRequest
from config import settings
from services import analyze_service
from services.analysis_config_service import (
    ANALYSIS_CONFIG_VERSION,
    PreparedAnalysisRequest,
    analysis_config_service,
)


def test_analysis_config_template_response_contains_defaults() -> None:
    template = cast(dict[str, Any], analysis_config_service.template_response())

    assert template["version"] == ANALYSIS_CONFIG_VERSION
    assert template["file_name"] == "resolver-rca-defaults.yaml"
    assert "request:" in template["template_yaml"]
    assert "constants:" in template["template_yaml"]
    assert "settings:" in template["template_yaml"]
    assert template["defaults"]["request"]["step"] == "15s"
    assert template["defaults"]["constants"]["default_metric_queries"]
    assert "mad_threshold" in template["defaults"]["settings"]


def test_prepare_request_applies_yaml_request_and_runtime_overrides() -> None:
    req = AnalyzeRequest(
        tenant_id="tenant-a",
        start=10,
        end=20,
        config_yaml="""
version: 1
request:
    step: 30s
    sensitivity: 4.5
constants:
    default_metric_queries:
        - custom_cpu
        - custom_latency
    default_weights:
        metrics: 0.7
        logs: 0.2
        traces: 0.1
    registry_alpha: 0.4
    default_service_name: checkout
settings:
    mad_threshold: 7.5
    analyze_timeout_seconds: 123
""",
    )

    prepared = analysis_config_service.prepare_request(req)

    assert prepared.request.step == "30s"
    assert prepared.request.sensitivity == 4.5
    assert prepared.constant_overrides["DEFAULT_METRIC_QUERIES"] == ["custom_cpu", "custom_latency"]
    assert prepared.constant_overrides["DEFAULT_WEIGHTS"] == {"metrics": 0.7, "logs": 0.2, "traces": 0.1}
    assert prepared.constant_overrides["REGISTRY_ALPHA"] == 0.4
    assert prepared.constant_overrides["DEFAULT_SERVICE_NAME"] == "checkout"
    assert prepared.settings_overrides["mad_threshold"] == 7.5
    assert prepared.timeout_seconds == 123.0


def test_prepared_request_timeout_seconds_falls_back_for_bool_and_invalid_types() -> None:
    request = AnalyzeRequest(tenant_id="tenant-a", start=1, end=2)

    prepared_bool = PreparedAnalysisRequest(
        request=request,
        settings_overrides={"analyze_timeout_seconds": True},
        constant_overrides={},
        explicit_fields=set(),
    )
    prepared_object = PreparedAnalysisRequest(
        request=request,
        settings_overrides={"analyze_timeout_seconds": object()},
        constant_overrides={},
        explicit_fields=set(),
    )

    expected_timeout = float(settings.analyze_timeout_seconds)

    assert prepared_bool.timeout_seconds == expected_timeout
    assert prepared_object.timeout_seconds == expected_timeout


def test_prepare_request_prefers_explicit_request_fields_over_yaml() -> None:
    req = AnalyzeRequest(
        tenant_id="tenant-a",
        start=10,
        end=20,
        step="45s",
        sensitivity=5.0,
        config_yaml="""
version: 1
request:
  step: 30s
  sensitivity: 2.5
""",
    )

    prepared = analysis_config_service.prepare_request(req, explicit_fields={"step", "sensitivity"})

    assert prepared.request.step == "45s"
    assert prepared.request.sensitivity == 5.0


@pytest.mark.parametrize(
    "raw_yaml, expected",
    [
        ("[", "Invalid RCA config YAML"),
        ("version: 1\nconstants:\n  unknown_key: 1\n", "Unknown analysis config constant override"),
        ("version: 1\nsettings:\n  host: 127.0.0.1\n", "Unknown analysis config setting override"),
    ],
)
def test_prepare_request_rejects_invalid_yaml(raw_yaml: str, expected: str) -> None:
    req = AnalyzeRequest(tenant_id="tenant-a", start=1, end=2, config_yaml=raw_yaml)

    with pytest.raises(HTTPException, match=expected):
        analysis_config_service.prepare_request(req)


@pytest.mark.parametrize(
    "raw_yaml, expected",
    [
        ("version: 1\nconstants:\n  default_metric_queries: invalid\n", "must be a list of strings"),
        ("version: 1\nconstants:\n  default_metric_queries:\n    - ''\n", "entries must be non-empty strings"),
        ("version: 1\nconstants:\n  forecast_thresholds: invalid\n", "must be a mapping of numeric values"),
        ("version: 1\nconstants:\n  forecast_thresholds:\n    '': 1\n", "keys must be non-empty strings"),
        ("version: 1\nconstants:\n  forecast_thresholds:\n    cpu: bad\n", "must be numeric"),
        ("version: 1\nconstants:\n  forecast_thresholds:\n    cpu: .inf\n", "must be finite"),
        ("version: 1\nconstants:\n  severity_weights: invalid\n", "must be a mapping of integer values"),
        ("version: 1\nconstants:\n  severity_weights:\n    '': 1\n", "keys must be non-empty strings"),
        ("version: 1\nconstants:\n  severity_weights:\n    cpu: true\n", "must be an integer"),
        ("version: 1\nconstants:\n  severity_weights:\n    cpu: nope\n", "must be an integer"),
        ("version: 1\nconstants:\n  registry_alpha: true\n", "registry_alpha must be numeric"),
        ("version: 1\nconstants:\n  registry_alpha: []\n", "registry_alpha must be numeric"),
        ("version: 1\nconstants:\n  registry_alpha: nope\n", "registry_alpha must be numeric"),
        ("version: 1\nconstants:\n  registry_alpha: .inf\n", "registry_alpha must be finite"),
        ("version: 1\nconstants:\n  default_service_name: ''\n", "must be a non-empty string"),
        ("version: 1\nsettings:\n  quality_min_corroboration_signals: nope\n", "validation error"),
        ("- value\n", "must decode to a mapping"),
        ("version: 99\n", "Unsupported RCA config version"),
        ("extra: 1\n", "Extra inputs are not permitted"),
    ],
)
def test_prepare_request_rejects_invalid_config_shapes(raw_yaml: str, expected: str) -> None:
    req = AnalyzeRequest(tenant_id="tenant-a", start=1, end=2, config_yaml=raw_yaml)

    with pytest.raises(HTTPException, match=expected):
        analysis_config_service.prepare_request(req)


def test_prepare_request_treats_null_yaml_as_defaults() -> None:
    prepared = analysis_config_service.prepare_request(
        AnalyzeRequest(tenant_id="tenant-a", start=1, end=2, config_yaml="null\n")
    )

    assert prepared.request.step == "15s"
    assert prepared.has_runtime_overrides is False


@pytest.mark.asyncio
async def test_apply_runtime_overrides_noop_for_default_request() -> None:
    prepared = analysis_config_service.prepare_request(AnalyzeRequest(tenant_id="tenant-a", start=1, end=2))

    async with analysis_config_service.apply_runtime_overrides(prepared):
        assert prepared.has_runtime_overrides is False


@pytest.mark.asyncio
async def test_run_analysis_applies_and_restores_runtime_overrides(monkeypatch: pytest.MonkeyPatch) -> None:
    import engine.analyzer as analyzer_module
    import engine.enums as enums_module
    import engine.registry as registry_module

    original_metric_queries = copy.deepcopy(analyzer_module.DEFAULT_METRIC_QUERIES)
    original_default_weights = copy.deepcopy(registry_module.DEFAULT_WEIGHTS)
    original_severity_weights = copy.deepcopy(enums_module.SEVERITY_WEIGHTS)
    original_mad_threshold = analyzer_module.settings.mad_threshold

    observed: dict[str, object] = {}

    monkeypatch.setattr(analyze_service, "enforce_request_tenant", lambda req: req)
    monkeypatch.setattr(analyze_service, "get_provider", lambda tenant_id: f"provider:{tenant_id}")

    async def fake_run(provider, req):
        observed["provider"] = provider
        observed["tenant_id"] = req.tenant_id
        observed["metric_queries"] = list(analyzer_module.DEFAULT_METRIC_QUERIES)
        observed["default_weights"] = dict(registry_module.DEFAULT_WEIGHTS)
        observed["severity_weights"] = dict(enums_module.SEVERITY_WEIGHTS)
        observed["mad_threshold"] = analyzer_module.settings.mad_threshold
        return {"ok": True}

    monkeypatch.setattr(analyze_service, "run", fake_run)

    req = AnalyzeRequest(
        tenant_id="tenant-a",
        start=10,
        end=20,
        config_yaml="""
version: 1
constants:
  default_metric_queries:
    - override_metric
  default_weights:
    metrics: 0.8
    logs: 0.1
    traces: 0.1
  severity_weights:
    low: 1
    medium: 5
    high: 7
    critical: 11
settings:
  mad_threshold: 9.5
""",
    )

    result = await analyze_service.run_analysis(req)

    assert result == {"ok": True}
    assert observed == {
        "provider": "provider:tenant-a",
        "tenant_id": "tenant-a",
        "metric_queries": ["override_metric"],
        "default_weights": {"metrics": 0.8, "logs": 0.1, "traces": 0.1},
        "severity_weights": {"low": 1, "medium": 5, "high": 7, "critical": 11},
        "mad_threshold": 9.5,
    }
    assert original_metric_queries == analyzer_module.DEFAULT_METRIC_QUERIES
    assert original_default_weights == registry_module.DEFAULT_WEIGHTS
    assert original_severity_weights == enums_module.SEVERITY_WEIGHTS
    assert analyzer_module.settings.mad_threshold == original_mad_threshold


def test_get_provider_delegates_to_route_helper(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("api.routes.common.get_provider", lambda tenant_id: {"tenant_id": tenant_id})

    assert analyze_service.get_provider("tenant-a") == {"tenant_id": "tenant-a"}
