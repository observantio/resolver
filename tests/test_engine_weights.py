"""
Test Engine Weights logic for managing tenant-specific weights for different signal types, including default handling,
updates, resets, and sanitization of corrupt stored data.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
"""

import pytest

from engine.enums import Signal
from engine.ml.weights import SignalWeights


def test_signal_weights_update_normalization():
    w = SignalWeights()
    original = dict(w.weights)
    assert w.update_count == 0
    w.update(Signal.METRICS, True)
    assert w.update_count == 1
    assert w.weights[Signal.METRICS] > original[Signal.METRICS]
    assert abs(sum(w.weights.values()) - 1.0) < 1e-6
    w.update(Signal.LOGS, False)
    assert w.update_count == 2
    assert w.weights[Signal.LOGS] < original[Signal.LOGS]
    w.reset()
    assert w.update_count == 0
    assert w.weights == original


def test_weighted_confidence():
    w = SignalWeights()
    score = w.weighted_confidence(1.0, 1.0, 1.0)
    assert score == pytest.approx(1.0 * w.get(Signal.METRICS) + w.get(Signal.LOGS) + w.get(Signal.TRACES))
    assert w.get(Signal.EVENTS) == pytest.approx(1 / len(Signal))
