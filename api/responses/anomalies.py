"""
Anomaly response models.

Copyright (c) 2026 Stefan Kumarasinghe.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. See http://www.apache.org/licenses/LICENSE-2.0 for details.
"""

from __future__ import annotations

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
    expected_range: tuple[float, float]
    severity: Severity
    description: str
    iqr_score: float = 0.0
    tukey_outlier_class: str = "none"
