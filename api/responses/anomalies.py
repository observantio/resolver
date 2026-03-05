"""
Anomaly response models.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
"""

from __future__ import annotations

from typing import Tuple

from engine.enums import ChangeType, Severity

from .base import NpModel


class MetricAnomaly(NpModel):
    metric_name: str
    timestamp: float
    value: float
    change_type: ChangeType
    z_score: float
    mad_score: float
    isolation_score: float
    expected_range: Tuple[float, float]
    severity: Severity
    description: str
