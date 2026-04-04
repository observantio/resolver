"""
Shared SLO result models.
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
