"""
Weights and thresholds for machine learning models and heuristics used in anomaly detection, root cause analysis, and
severity classification, defined in a centralized module for easy configuration and tuning.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
"""

from __future__ import annotations

from dataclasses import dataclass, field
from collections.abc import Mapping
from typing import Dict, Union

from engine.enums import Signal
from config import DEFAULT_WEIGHTS, REGISTRY_ALPHA

from config import settings

if settings.default_weight_fallback and settings.default_weight_fallback > 0.0:
    _DEFAULT_FALLBACK = settings.default_weight_fallback
else:
    _DEFAULT_FALLBACK = 1.0 / len(Signal)


def _key(signal: Union[Signal, str]) -> str:
    return signal.value if isinstance(signal, Signal) else signal


def _coerce_weight(value: object) -> float:
    if isinstance(value, bool):
        raise TypeError("bool is not a valid weight")
    if isinstance(value, (int, float, str)):
        return float(value)
    raise TypeError(f"Unsupported weight value: {type(value).__name__}")


def _normalise_weights(raw: Mapping[Signal | str, object]) -> Dict[str, float]:
    return {_key(k): _coerce_weight(v) for k, v in raw.items()}


@dataclass
class SignalWeights:
    weights: Dict[str, float] = field(default_factory=lambda: _normalise_weights(DEFAULT_WEIGHTS))
    alpha: float = REGISTRY_ALPHA
    update_count: int = 0

    def update(self, signal: Union[Signal, str], was_correct: bool) -> None:
        k = _key(signal)
        reward = 1.0 if was_correct else 0.0
        current = self.weights.get(k, _DEFAULT_FALLBACK)
        self.weights[k] = (1 - self.alpha) * current + self.alpha * reward
        self._normalize()
        self.update_count += 1

    def _normalize(self) -> None:
        total = sum(self.weights.values()) or 1.0
        for k in self.weights:
            self.weights[k] = self.weights[k] / total

    def get(self, signal: Union[Signal, str]) -> float:
        return self.weights.get(_key(signal), _DEFAULT_FALLBACK)

    def weighted_confidence(
        self,
        metric_score: float,
        log_score: float,
        trace_score: float,
    ) -> float:
        return round(
            self.get(Signal.METRICS) * metric_score
            + self.get(Signal.LOGS) * log_score
            + self.get(Signal.TRACES) * trace_score,
            4,
        )

    def reset(self) -> None:
        self.weights = _normalise_weights(DEFAULT_WEIGHTS)
        self.update_count = 0

    def load(self, raw: Mapping[Signal | str, object]) -> None:
        self.weights = _normalise_weights(raw)


_global_weights = SignalWeights()


def get_weights() -> SignalWeights:
    return _global_weights
