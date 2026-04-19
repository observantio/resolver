"""
Helpers for RCA job configuration overrides driven by uploaded YAML.

This module keeps per-job analysis tuning scoped to the current RCA execution while preserving server defaults when no
YAML is supplied.

Copyright (c) 2026 Stefan Kumarasinghe.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. See http://www.apache.org/licenses/LICENSE-2.0 for details.
"""

from __future__ import annotations

import asyncio
import copy
import math
import sys
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from types import ModuleType

import yaml
from fastapi import HTTPException, status
from pydantic import BaseModel, ConfigDict, Field, ValidationError

import config as config_module
from api.requests import AnalyzeRequest
from config import Settings, settings

ANALYSIS_CONFIG_VERSION = 1
_OVERRIDABLE_CONSTANT_KEYS = {
    "default_metric_queries": "DEFAULT_METRIC_QUERIES",
    "forecast_thresholds": "FORECAST_THRESHOLDS",
    "slo_error_query": "SLO_ERROR_QUERY",
    "slo_total_query": "SLO_TOTAL_QUERY",
    "default_weights": "DEFAULT_WEIGHTS",
    "registry_alpha": "REGISTRY_ALPHA",
    "severity_weights": "SEVERITY_WEIGHTS",
    "default_service_name": "DEFAULT_SERVICE_NAME",
}
_NON_OVERRIDABLE_SETTINGS = {
    "logs_backend",
    "loki_url",
    "loki_labels",
    "loki_timeout",
    "loki_batch_size",
    "metrics_backend",
    "mimir_url",
    "traces_backend",
    "tempo_url",
    "connector_timeout",
    "startup_timeout",
    "host",
    "port",
    "expected_service_token",
    "context_verify_key",
    "context_issuer",
    "context_audience",
    "context_algorithms",
    "context_replay_ttl_seconds",
    "ssl_enabled",
    "ssl_certfile",
    "ssl_keyfile",
    "database_url",
    "analyze_max_concurrency",
    "analyze_report_retention_days",
    "analyze_job_ttl_days",
    "default_tenant_id",
}
_REQUEST_OVERRIDE_FIELDS = (
    "step",
    "sensitivity",
    "apdex_threshold_ms",
    "slo_target",
    "correlation_window_seconds",
    "forecast_horizon_seconds",
)
_MODULE_PREFIXES = (
    "api.",
    "engine.",
    "services.",
    "store.",
    "main",
    "config",
)


class _RequestOverrideModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    step: str | None = None
    sensitivity: float | None = Field(default=None, ge=1.0, le=6.0)
    apdex_threshold_ms: float | None = None
    slo_target: float | None = Field(default=None, ge=0.0, le=1.0)
    correlation_window_seconds: float | None = Field(default=None, ge=10.0, le=600.0)
    forecast_horizon_seconds: float | None = Field(default=None, ge=60.0, le=86400.0)


class _ConfigDocumentModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    version: int = ANALYSIS_CONFIG_VERSION
    request: _RequestOverrideModel = Field(default_factory=_RequestOverrideModel)
    constants: dict[str, object] = Field(default_factory=dict)
    settings: dict[str, object] = Field(default_factory=dict)


@dataclass(frozen=True)
class PreparedAnalysisRequest:
    request: AnalyzeRequest
    settings_overrides: dict[str, object]
    constant_overrides: dict[str, object]
    explicit_fields: set[str]

    @property
    def timeout_seconds(self) -> float:
        timeout = self.settings_overrides.get("analyze_timeout_seconds", settings.analyze_timeout_seconds)
        if isinstance(timeout, bool):
            return float(settings.analyze_timeout_seconds)
        if isinstance(timeout, (int, float, str)):
            return float(timeout)
        return float(settings.analyze_timeout_seconds)

    @property
    def has_runtime_overrides(self) -> bool:
        return bool(self.settings_overrides or self.constant_overrides)


def _copy_value(value: object) -> object:
    return copy.deepcopy(value)


def _analysis_settings_defaults() -> dict[str, object]:
    dumped = settings.model_dump()
    return {key: _copy_value(value) for key, value in dumped.items() if key not in _NON_OVERRIDABLE_SETTINGS}


def _analysis_constant_defaults() -> dict[str, object]:
    return {
        "default_metric_queries": list(config_module.DEFAULT_METRIC_QUERIES),
        "forecast_thresholds": dict(config_module.FORECAST_THRESHOLDS),
        "slo_error_query": str(config_module.SLO_ERROR_QUERY),
        "slo_total_query": str(config_module.SLO_TOTAL_QUERY),
        "default_weights": dict(config_module.DEFAULT_WEIGHTS),
        "registry_alpha": float(config_module.REGISTRY_ALPHA),
        "severity_weights": dict(config_module.SEVERITY_WEIGHTS),
        "default_service_name": str(config_module.DEFAULT_SERVICE_NAME),
    }


def _request_defaults() -> dict[str, object]:
    default_request = AnalyzeRequest(tenant_id="template", start=0, end=1)
    return {
        "step": str(default_request.step),
        "sensitivity": _copy_value(default_request.sensitivity),
        "apdex_threshold_ms": _copy_value(default_request.apdex_threshold_ms),
        "slo_target": _copy_value(default_request.slo_target),
        "correlation_window_seconds": _copy_value(default_request.correlation_window_seconds),
        "forecast_horizon_seconds": _copy_value(default_request.forecast_horizon_seconds),
    }


def _template_defaults() -> dict[str, object]:
    return {
        "version": ANALYSIS_CONFIG_VERSION,
        "request": _request_defaults(),
        "constants": _analysis_constant_defaults(),
        "settings": _analysis_settings_defaults(),
    }


def _http_400(detail: str) -> HTTPException:
    return HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=detail)


def _normalize_string_list(name: str, value: object) -> list[str]:
    if not isinstance(value, list):
        raise _http_400(f"analysis config constants.{name} must be a list of strings")
    normalized: list[str] = []
    for item in value:
        text = str(item or "").strip()
        if not text:
            raise _http_400(f"analysis config constants.{name} entries must be non-empty strings")
        normalized.append(text)
    return normalized


def _normalize_float_dict(name: str, value: object) -> dict[str, float]:
    if not isinstance(value, dict):
        raise _http_400(f"analysis config constants.{name} must be a mapping of numeric values")
    normalized: dict[str, float] = {}
    for key, raw in value.items():
        metric = str(key or "").strip()
        if not metric:
            raise _http_400(f"analysis config constants.{name} keys must be non-empty strings")
        try:
            numeric = float(raw)
        except (TypeError, ValueError) as exc:
            raise _http_400(f"analysis config constants.{name}.{metric} must be numeric") from exc
        if not math.isfinite(numeric):
            raise _http_400(f"analysis config constants.{name}.{metric} must be finite")
        normalized[metric] = numeric
    return normalized


def _normalize_int_dict(name: str, value: object) -> dict[str, int]:
    if not isinstance(value, dict):
        raise _http_400(f"analysis config constants.{name} must be a mapping of integer values")
    normalized: dict[str, int] = {}
    for key, raw in value.items():
        metric = str(key or "").strip()
        if not metric:
            raise _http_400(f"analysis config constants.{name} keys must be non-empty strings")
        if isinstance(raw, bool):
            raise _http_400(f"analysis config constants.{name}.{metric} must be an integer")
        try:
            numeric = int(raw)
        except (TypeError, ValueError) as exc:
            raise _http_400(f"analysis config constants.{name}.{metric} must be an integer") from exc
        normalized[metric] = numeric
    return normalized


def _normalize_constant_overrides(raw: dict[str, object]) -> dict[str, object]:
    unknown = sorted(set(raw) - set(_OVERRIDABLE_CONSTANT_KEYS))
    if unknown:
        raise _http_400(f"Unknown analysis config constant override(s): {', '.join(unknown)}")

    normalized: dict[str, object] = {}
    for key, value in raw.items():
        target = _OVERRIDABLE_CONSTANT_KEYS[key]
        if key == "default_metric_queries":
            normalized[target] = _normalize_string_list(key, value)
        elif key in {"forecast_thresholds", "default_weights"}:
            normalized[target] = _normalize_float_dict(key, value)
        elif key == "severity_weights":
            normalized[target] = _normalize_int_dict(key, value)
        elif key == "registry_alpha":
            if isinstance(value, bool):
                raise _http_400("analysis config constants.registry_alpha must be numeric")
            if not isinstance(value, (int, float, str)):
                raise _http_400("analysis config constants.registry_alpha must be numeric")
            try:
                numeric = float(value)
            except (TypeError, ValueError) as exc:
                raise _http_400("analysis config constants.registry_alpha must be numeric") from exc
            if not math.isfinite(numeric):
                raise _http_400("analysis config constants.registry_alpha must be finite")
            normalized[target] = numeric
        else:
            text = str(value or "").strip()
            if not text:
                raise _http_400(f"analysis config constants.{key} must be a non-empty string")
            normalized[target] = text
    return normalized


def _normalize_settings_overrides(raw: dict[str, object]) -> dict[str, object]:
    unknown = sorted(set(raw) - set(_analysis_settings_defaults()))
    if unknown:
        raise _http_400(f"Unknown analysis config setting override(s): {', '.join(unknown)}")

    baseline = settings.model_dump()
    baseline.update(raw)
    try:
        validated = Settings.model_validate(baseline)
    except ValidationError as exc:
        raise _http_400(str(exc)) from exc
    return {key: _copy_value(getattr(validated, key)) for key in raw}


class AnalysisConfigService:
    def __init__(self) -> None:
        self._runtime_lock = asyncio.Lock()

    def template_response(self) -> dict[str, object]:
        defaults = _template_defaults()
        return {
            "version": ANALYSIS_CONFIG_VERSION,
            "defaults": defaults,
            "template_yaml": yaml.safe_dump(defaults, sort_keys=False, allow_unicode=False),
            "file_name": "resolver-rca-defaults.yaml",
        }

    def _parse_document(self, raw_yaml: str | None) -> _ConfigDocumentModel:
        if raw_yaml is None or not str(raw_yaml).strip():
            return _ConfigDocumentModel()
        try:
            loaded = yaml.safe_load(raw_yaml)
        except yaml.YAMLError as exc:
            raise _http_400(f"Invalid RCA config YAML: {exc}") from exc
        if loaded is None:
            return _ConfigDocumentModel()
        if not isinstance(loaded, dict):
            raise _http_400("RCA config YAML must decode to a mapping")
        try:
            document = _ConfigDocumentModel.model_validate(loaded)
        except ValidationError as exc:
            raise _http_400(str(exc)) from exc
        if document.version != ANALYSIS_CONFIG_VERSION:
            raise _http_400(f"Unsupported RCA config version {document.version}; expected {ANALYSIS_CONFIG_VERSION}")
        return document

    def prepare_request(
        self,
        req: AnalyzeRequest,
        *,
        explicit_fields: set[str] | None = None,
    ) -> PreparedAnalysisRequest:
        document = self._parse_document(getattr(req, "config_yaml", None))
        request_updates = document.request.model_dump(exclude_none=True)
        direct_fields = set(explicit_fields if explicit_fields is not None else req.model_fields_set)
        for field_name in _REQUEST_OVERRIDE_FIELDS:
            if field_name in direct_fields:
                request_updates[field_name] = getattr(req, field_name)
        prepared_request = req.model_copy(update=request_updates)
        return PreparedAnalysisRequest(
            request=prepared_request,
            settings_overrides=_normalize_settings_overrides(document.settings),
            constant_overrides=_normalize_constant_overrides(document.constants),
            explicit_fields=direct_fields,
        )

    @staticmethod
    def _runtime_modules() -> list[ModuleType]:
        modules: list[ModuleType] = []
        for module in sys.modules.values():
            name = getattr(module, "__name__", "")
            if name == "config" or name.startswith(_MODULE_PREFIXES):
                modules.append(module)
        return modules

    @asynccontextmanager
    async def apply_runtime_overrides(self, prepared: PreparedAnalysisRequest) -> AsyncIterator[None]:
        if not prepared.has_runtime_overrides:
            yield
            return

        async with self._runtime_lock:
            original_setting_values = {key: _copy_value(getattr(settings, key)) for key in prepared.settings_overrides}
            original_constants: list[tuple[ModuleType, str, object]] = []
            modules = self._runtime_modules()
            try:
                for key, value in prepared.settings_overrides.items():
                    setattr(settings, key, _copy_value(value))

                for module in modules:
                    for name, value in prepared.constant_overrides.items():
                        if hasattr(module, name):
                            original_constants.append((module, name, _copy_value(getattr(module, name))))
                            setattr(module, name, _copy_value(value))
                yield
            finally:
                for module, name, value in reversed(original_constants):
                    setattr(module, name, value)
                for key, value in original_setting_values.items():
                    setattr(settings, key, value)


analysis_config_service = AnalysisConfigService()
