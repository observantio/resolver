"""
Test Suite for SLO Burn and Budget Calculations.

Copyright (c) 2026 Stefan Kumarasinghe.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. See http://www.apache.org/licenses/LICENSE-2.0 for details.
"""

from engine.slo.budget import BudgetStatus, remaining_minutes
from engine.slo.burn import SloBurnAlert, evaluate


def test_slo_evaluate_empty():
    assert evaluate("svc", [], [], [], target_availability=0.99) == []


def test_slo_evaluate_burn():
    ts = [0, 3600]
    total = [100, 100]
    errors = [10, 20]
    alerts = evaluate("svc", errors, total, ts, target_availability=0.9)
    assert isinstance(alerts, list)
    if alerts:
        assert isinstance(alerts[0], SloBurnAlert)
        assert alerts[0].burn_rate > 0


def test_budget_remaining():
    status = remaining_minutes("svc", [0], [0], 0.99)
    assert isinstance(status, BudgetStatus)
    assert status.current_availability == 1.0
    status2 = remaining_minutes("svc", [10], [100], 0.99)
    assert status2.budget_used_pct >= 0
