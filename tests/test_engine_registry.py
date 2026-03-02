"""
Test engine registry logic for managing tenant-specific weights for different signal types, including default handling, updates, resets, and sanitization of corrupt stored data.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
"""

import pytest

from engine import registry as ereg
from engine.enums import Signal
from store import weights as wstore


@pytest.mark.asyncio
async def test_engine_registry_defaults_and_updates(monkeypatch):
    tid = "tenantX"
    saved = {}

    async def fake_load(t):
        return saved.get(t)

    async def fake_save(t, weights, update_count):
        saved[t] = {"weights": weights, "update_count": update_count}

    async def fake_delete(t):
        saved.pop(t, None)

    monkeypatch.setattr(wstore, "load", fake_load)
    monkeypatch.setattr(wstore, "save", fake_save)
    monkeypatch.setattr(wstore, "delete", fake_delete)

    reg = ereg.TenantRegistry()

    # initial state should coerce DEFAULT_WEIGHTS strings to Signal keys
    state = await reg.get_state(tid)
    assert state.update_count == 0
    # weights property returns internal representation (Signal keys)
    assert set(state.weights.keys()) == {Signal.metrics, Signal.logs, Signal.traces}
    # serializable form exposes string values
    assert state.weights_serializable == {"metrics": 0.3, "logs": 0.35, "traces": 0.35}

    # calling update_weight with a string should be accepted
    await reg.update_weight(tid, "metrics", True)
    assert saved[tid]["weights"]["metrics"] > 0.3
    assert saved[tid]["update_count"] == 1

    # the state still internally uses Signal key
    state2 = await reg.get_state(tid)
    assert Signal.metrics in state2.weights

    # increasing the count and resetting
    state2.update_weight(Signal.logs, False)
    assert state2.update_count == 2
    await reg.reset_weights(tid)
    state3 = await reg.get_state(tid)
    assert state3.update_count == 0
    assert state3.weights_serializable == {"metrics": 0.3, "logs": 0.35, "traces": 0.35}

    # ensure weighted_confidence returns weighted sum using current state
    base = state3.weighted_confidence(1.0, 1.0, 1.0)
    # since defaults sum to 1, should equal 1.0
    assert base == pytest.approx(1.0)
    # simulate skewing weights and confirm computation changes
    state3._weights[Signal.metrics] = 1.0
    state3._weights[Signal.logs] = 0.0
    state3._weights[Signal.traces] = 0.0
    assert state3.weighted_confidence(0.5, 0.5, 0.5) == pytest.approx(0.5)


@pytest.mark.asyncio
async def test_engine_registry_sanitizes_corrupt_stored_weights(monkeypatch):
    tid = "tenant-corrupt"

    async def fake_load(_):
        return {
            "weights": {
                "metrics": "nan",
                "logs": -2.0,
                "traces": 0.6,
                "unknown": 123,
            },
            "update_count": "bad",
        }

    async def fake_save(*_args, **_kwargs):
        return None

    async def fake_delete(*_args, **_kwargs):
        return None

    monkeypatch.setattr(wstore, "load", fake_load)
    monkeypatch.setattr(wstore, "save", fake_save)
    monkeypatch.setattr(wstore, "delete", fake_delete)

    reg = ereg.TenantRegistry()
    state = await reg.get_state(tid)
    weights = state.weights_serializable
    assert set(weights.keys()) == {"metrics", "logs", "traces"}
    assert all(v >= 0.0 for v in weights.values())
    assert abs(sum(weights.values()) - 1.0) < 1e-6
    assert state.update_count == 0
