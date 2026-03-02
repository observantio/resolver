"""
Test cases for forecast logic in the analysis engine, including linear fitting, R-squared calculation, and trajectory forecasting with various thresholds and edge cases.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
"""

import pytest

from config import settings
from engine.forecast.trajectory import _linear_fit, _r_squared, forecast, TrajectoryForecast


def test_linear_fit_and_r2():
    ts = [0, 1, 2, 3, 4]
    vals = [1, 2, 3, 4, 5]
    slope, intercept = _linear_fit(ts, vals)
    assert pytest.approx(slope, rel=1e-3) == 1.0
    r2 = _r_squared(ts, vals, slope, intercept)
    assert pytest.approx(r2, rel=1e-3) == 1.0


def test_forecast_insufficient(monkeypatch):
    
    monkeypatch.setattr(settings, "forecast_trajectory_min_length", 5)
    assert forecast("m", [0, 1, 2, 3], [1, 2, 3, 4], threshold=10) is None

    
    monkeypatch.setattr(settings, "forecast_trajectory_min_length", 3)
    assert forecast("m", [0, 1, 2, 3], [1, 2, 3, 4], threshold=10) is not None


def test_forecast_no_r2():
    ts = list(range(10))
    vals = [1] * 10  
    assert forecast("m", ts, vals, threshold=2) is None


def test_forecast_breach():
    ts = list(range(20))
    vals = [i for i in range(20)]
    res = forecast("m", ts, vals, threshold=25, horizon_seconds=10)
    assert isinstance(res, TrajectoryForecast)
    assert res.severity in {res.severity,}
    assert res.current_value < res.predicted_value_at_horizon


def test_forecast_r2_threshold(monkeypatch):
    ts = list(range(10))
    
    vals = [i + (2 if i == 5 else 0) for i in range(10)]
    
    
    threshold = 25
    horizon = 10

    
    monkeypatch.setattr(settings, "forecast_trajectory_r2_threshold", 0.99)
    assert forecast("m", ts, vals, threshold=threshold, horizon_seconds=horizon) is None
    
    monkeypatch.setattr(settings, "forecast_trajectory_r2_threshold", 0.0)
    assert forecast("m", ts, vals, threshold=threshold, horizon_seconds=horizon) is not None
