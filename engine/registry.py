"""
Registry for Tenant-Specific Weights and Events.

Copyright (c) 2026 Stefan Kumarasinghe.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. See http://www.apache.org/licenses/LICENSE-2.0 for details.
"""

from __future__ import annotations

import logging
import math

from config import DEFAULT_WEIGHTS, REGISTRY_ALPHA
from engine.enums import Signal
from engine.events.registry import DeploymentEvent
from store import events as event_store
from store import weights as weight_store

log = logging.getLogger(__name__)

SIGNAL_KEYS: tuple[Signal, ...] = (Signal.METRICS, Signal.LOGS, Signal.TRACES)


def _default_weights() -> dict[Signal, float]:
    defaults: dict[Signal, float] = {}
    for signal in SIGNAL_KEYS:
        configured = DEFAULT_WEIGHTS.get(signal.value)
        if isinstance(configured, (int, float)) and math.isfinite(float(configured)) and float(configured) >= 0.0:
            defaults[signal] = float(configured)
        else:
            defaults[signal] = 1.0 / len(SIGNAL_KEYS)
    total = sum(defaults.values()) or 1.0
    for signal in defaults:
        defaults[signal] = defaults[signal] / total
    return defaults


def _coerce_weights(raw: object) -> dict[Signal, float]:
    weights = _default_weights()
    if not isinstance(raw, dict):
        return weights

    for key, value in raw.items():
        signal: Signal | None = None
        if isinstance(key, Signal):
            signal = key
        elif isinstance(key, str):
            try:
                signal = Signal(key)
            except ValueError:
                signal = None
        if signal is None:
            continue

        try:
            numeric = float(value)
        except (TypeError, ValueError):
            continue
        if not math.isfinite(numeric) or numeric < 0.0:
            continue
        weights[signal] = numeric

    total = sum(weights.values()) or 1.0
    for signal in weights:
        weights[signal] = weights[signal] / total
    return weights


def _serialize_weights(weights: dict[Signal, float]) -> dict[str, float]:
    return {k.value: v for k, v in weights.items()}


def _coerce_update_count(value: object) -> int:
    parsed = 0
    if isinstance(value, bool):
        parsed = int(value)
    elif isinstance(value, int):
        parsed = value
    elif isinstance(value, float) and math.isfinite(value):
        parsed = int(value)
    elif isinstance(value, str):
        try:
            parsed = int(value)
        except ValueError:
            parsed = 0
    return max(0, parsed)


class TenantState:
    __slots__ = ("_update_count", "_weights")

    def __init__(self, weights: dict[Signal, float], update_count: int) -> None:
        self._weights: dict[Signal, float] = _coerce_weights(weights)
        self._update_count = update_count

    @property
    def weights(self) -> dict[Signal, float]:
        return dict(self._weights)

    @property
    def weights_serializable(self) -> dict[str, float]:
        return _serialize_weights(self._weights)

    @property
    def update_count(self) -> int:
        return self._update_count

    def update_weight(self, signal: Signal, was_correct: bool) -> None:
        reward = 1.0 if was_correct else 0.0
        current = self._weights.get(signal, _default_weights().get(signal, 1.0 / len(SIGNAL_KEYS)))
        self._weights[signal] = (1 - REGISTRY_ALPHA) * current + REGISTRY_ALPHA * reward
        self._normalize()
        self._update_count += 1

    def weighted_confidence(
        self,
        metric_score: float,
        log_score: float,
        trace_score: float,
    ) -> float:
        """
        Return a confidence value computed as a weighted sum of the three signal-specific scores using the tenant's
        adaptive weights.

        The result is capped according to ``settings.correlation_score_max``
        when used in temporal correlation.  This helper is consumed by
        ``engine.correlation.temporal.correlate`` and other components that
        need to combine per-signal scores according to tenant preferences.
        """
        w = self._weights
        default = _default_weights()
        return round(
            w.get(Signal.METRICS, default.get(Signal.METRICS, 1.0 / len(SIGNAL_KEYS))) * metric_score
            + w.get(Signal.LOGS, default.get(Signal.LOGS, 1.0 / len(SIGNAL_KEYS))) * log_score
            + w.get(Signal.TRACES, default.get(Signal.TRACES, 1.0 / len(SIGNAL_KEYS))) * trace_score,
            4,
        )

    def _normalize(self) -> None:
        total = sum(self._weights.values()) or 1.0
        for k in self._weights:
            self._weights[k] = self._weights[k] / total

    def reset(self) -> None:
        self._weights = _coerce_weights(DEFAULT_WEIGHTS)
        self._update_count = 0


class TenantRegistry:
    def __init__(self) -> None:
        self._states: dict[str, TenantState] = {}

    async def get_state(self, tenant_id: str) -> TenantState:
        if tenant_id not in self._states:
            stored = await weight_store.load(tenant_id)
            if stored:
                state = TenantState(
                    weights=_coerce_weights(stored.get("weights")),
                    update_count=_coerce_update_count(stored.get("update_count", 0)),
                )
            else:
                state = TenantState(weights=_coerce_weights(DEFAULT_WEIGHTS), update_count=0)
            self._states[tenant_id] = state
        return self._states[tenant_id]

    async def update_weight(self, tenant_id: str, signal: Signal | str, was_correct: bool) -> TenantState:
        if isinstance(signal, str):
            signal = Signal(signal)
        state = await self.get_state(tenant_id)
        state.update_weight(signal, was_correct)
        await weight_store.save(tenant_id, state.weights_serializable, state.update_count)
        return state

    async def reset_weights(self, tenant_id: str) -> TenantState:
        state = await self.get_state(tenant_id)
        state.reset()
        await weight_store.delete(tenant_id)
        self.evict(tenant_id)
        return state

    async def register_event(self, tenant_id: str, event: DeploymentEvent) -> None:
        await event_store.append(tenant_id, event)

    async def get_events(self, tenant_id: str) -> list[event_store.StoredEvent]:
        return await event_store.load(tenant_id)

    async def clear_events(self, tenant_id: str) -> None:
        await event_store.clear(tenant_id)

    async def events_in_window(self, tenant_id: str, start: float, end: float) -> list[event_store.StoredEvent]:
        events = await event_store.load(tenant_id)
        return [e for e in events if start <= e["timestamp"] <= end]

    def evict(self, tenant_id: str) -> None:
        self._states.pop(tenant_id, None)


_registry = TenantRegistry()


def get_registry() -> TenantRegistry:
    return _registry
