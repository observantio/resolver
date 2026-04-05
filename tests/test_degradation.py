"""
Test cases for degradation analysis logic in the analysis engine, including EMA and acceleration calculations, trend
classification, and severity assessment.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
"""

from engine.forecast.degradation import _ema, _acceleration, analyze
from engine.enums import Severity


def test_ema_and_acceleration():
    vals = [1, 2, 3, 4, 5]
    ema = _ema(vals, alpha=0.5)
    assert len(ema) == len(vals)
    acc = _acceleration(ema)
    assert isinstance(acc, float)


def test_analyze_none_short():
    assert analyze("m", [0, 1, 2], [1, 2, 3]) is None


def test_analyze_degrading():
    ts = list(range(20))
    vals = [i * 2 for i in ts]
    sig = analyze("m", ts, vals)
    assert sig is not None
    assert sig.trend == "degrading"
    assert sig.severity in Severity
