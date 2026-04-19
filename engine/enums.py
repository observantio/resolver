"""
Enumerations for Severity, Signal Types, Change Types, and RCA Categories.

Copyright (c) 2026 Stefan Kumarasinghe.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. See http://www.apache.org/licenses/LICENSE-2.0 for details.
"""

from __future__ import annotations

from enum import Enum

from config import SEVERITY_WEIGHTS, settings


class Severity(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"

    @classmethod
    def from_score(cls, score: float) -> Severity:
        if score >= settings.severity_score_critical:
            return cls.CRITICAL
        if score >= settings.severity_score_high:
            return cls.HIGH
        if score >= settings.severity_score_medium:
            return cls.MEDIUM
        return cls.LOW

    def weight(self) -> int:
        return SEVERITY_WEIGHTS[self.value]


class Signal(str, Enum):
    METRICS = "metrics"
    LOGS = "logs"
    TRACES = "traces"
    EVENTS = "events"


class ChangeType(str, Enum):
    SPIKE = "spike"
    DROP = "drop"
    DRIFT = "drift"
    SHIFT = "shift"
    OSCILLATION = "oscillation"


class RcaCategory(str, Enum):
    DEPLOYMENT = "deployment"
    RESOURCE_EXHAUSTION = "resource_exhaustion"
    DEPENDENCY_FAILURE = "dependency_failure"
    TRAFFIC_SURGE = "traffic_surge"
    ERROR_PROPAGATION = "error_propagation"
    SLO_BURN = "slo_burn"
    UNKNOWN = "unknown"
