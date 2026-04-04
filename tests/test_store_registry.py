"""
Test Suite for Store Client.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
"""

import pytest

from engine.enums import Signal
from engine import registry as sreg
from store import weights as wstore


@pytest.mark.asyncio
async def test_registry_state_update_and_reset(monkeypatch):
    tid = "tenantA"

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

    state = await sreg.TenantRegistry().get_state(tid)
    assert state.update_count == 0
    assert state.weights_serializable == {"metrics": 0.3, "logs": 0.35, "traces": 0.35}

    await sreg.TenantRegistry().update_weight(tid, "metrics", True)
    assert saved[tid]["weights"]["metrics"] > 0.3
    assert saved[tid]["update_count"] == 1

    reg = sreg.TenantRegistry()
    state = await reg.get_state(tid)
    state.update_weight(Signal.LOGS, False)
    await reg.reset_weights(tid)
    state2 = await reg.get_state(tid)
    assert state2.update_count == 0
    assert state2.weights_serializable == {"metrics": 0.3, "logs": 0.35, "traces": 0.35}
