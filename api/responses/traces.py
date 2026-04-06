"""
Trace response models.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
"""

from __future__ import annotations

from engine.enums import Severity

from .base import NpModel


class ServiceLatency(NpModel):
    service: str
    operation: str
    p50_ms: float
    p95_ms: float
    p99_ms: float
    apdex: float
    error_rate: float
    sample_count: int
    severity: Severity
    window_start: float | None = None
    window_end: float | None = None


class ErrorPropagation(NpModel):
    source_service: str
    affected_services: list[str]
    error_rate: float
    severity: Severity
