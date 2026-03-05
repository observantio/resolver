"""
SLO response models.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
"""

from __future__ import annotations

from engine.enums import Severity

from .base import NpModel


class SloBurnAlert(NpModel):
    service: str
    window_label: str
    error_rate: float
    burn_rate: float
    budget_consumed_pct: float
    severity: Severity


class BudgetStatus(NpModel):
    service: str
    target_availability: float
    current_availability: float
    budget_used_pct: float
    remaining_minutes: float
    on_track: bool
