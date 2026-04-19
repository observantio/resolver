"""
Shared SLO result models.

Copyright (c) 2026 Stefan Kumarasinghe.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. See http://www.apache.org/licenses/LICENSE-2.0 for details.
"""

from __future__ import annotations

from dataclasses import dataclass

from engine.enums import Severity


@dataclass(frozen=True)
class SloBurnAlert:
    service: str
    window_label: str
    error_rate: float
    burn_rate: float
    budget_consumed_pct: float
    severity: Severity


@dataclass(frozen=True)
class BudgetStatus:
    service: str
    target_availability: float
    current_availability: float
    budget_used_pct: float
    remaining_minutes: float
    on_track: bool
