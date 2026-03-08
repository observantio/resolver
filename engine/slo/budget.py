"""
Budget analysis for SLOs, calculating remaining error budget and time based on current error rates and target availability.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
"""


from __future__ import annotations

from typing import List

from config import settings
from engine.slo.models import BudgetStatus


def remaining_minutes(
    service: str,
    error_counts: List[float],
    total_counts: List[float],
    target_availability: float = 0.999,
) -> BudgetStatus:
    total = sum(total_counts)
    errors = sum(error_counts)

    if total == 0:
        remaining = settings.slo_month_minutes * (1.0 - target_availability)
        return BudgetStatus(
            service=service,
            target_availability=target_availability,
            current_availability=1.0,
            budget_used_pct=0.0,
            remaining_minutes=round(remaining, 1),
            on_track=True,
        )

    current_avail = 1.0 - (errors / total)
    allowed_downtime = settings.slo_month_minutes * (1.0 - target_availability)
    used_downtime = settings.slo_month_minutes * (errors / total)
    remaining = max(0.0, allowed_downtime - used_downtime)
    budget_used = min(100.0, (used_downtime / allowed_downtime * 100.0) if allowed_downtime else 100.0)

    return BudgetStatus(
        service=service,
        target_availability=target_availability,
        current_availability=round(current_avail, 6),
        budget_used_pct=round(budget_used, 2),
        remaining_minutes=round(remaining, 1),
        on_track=budget_used < 100.0,
    )
