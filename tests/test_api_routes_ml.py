"""
Test API routes for machine learning analysis endpoints, validating request handling, response formatting, and integration with the analysis engine.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
"""

import pytest

from api.routes import ml as ml_route
from engine.enums import Signal


class DummyState:
    def __init__(self):

        self.weights_serializable = {"metrics": 0.3, "logs": 0.35, "traces": 0.35}
        self.update_count = 0


class DummyRegistry:
    def __init__(self):
        self.calls = []
        self.state = DummyState()

    async def update_weight(self, tenant_id, signal, was_correct):

        self.calls.append((tenant_id, signal, was_correct))

        self.state.update_count += 1
        self.state.weights_serializable = {"metrics": 0.5}
        return self.state

    async def get_state(self, tenant_id):
        return self.state

    async def reset_weights(self, tenant_id):

        self.state.update_count = 0
        self.state.weights_serializable = {"metrics": 0.3, "logs": 0.35, "traces": 0.35}
        return self.state


@pytest.mark.asyncio
async def test_signal_feedback_success(monkeypatch):
    dummy = DummyRegistry()
    monkeypatch.setattr(ml_route, "get_registry", lambda: dummy)

    res = await ml_route.signal_feedback("tenantA", "metrics", True)

    assert dummy.calls == [("tenantA", Signal.metrics, True)]
    assert res == {"updated_weights": {"metrics": 0.5}, "update_count": 1}


@pytest.mark.asyncio
async def test_signal_feedback_invalid(monkeypatch):
    dummy = DummyRegistry()
    monkeypatch.setattr(ml_route, "get_registry", lambda: dummy)

    with pytest.raises(Exception) as excinfo:
        await ml_route.signal_feedback("tenantA", "unknown", True)

    assert "Unknown signal" in str(excinfo.value)


@pytest.mark.asyncio
async def test_get_and_reset_weights(monkeypatch):
    dummy = DummyRegistry()
    dummy.state.weights_serializable = {"foo": 1.0}
    dummy.state.update_count = 5
    monkeypatch.setattr(ml_route, "get_registry", lambda: dummy)

    res = await ml_route.get_signal_weights("tenantX")
    assert res == {"weights": {"foo": 1.0}, "update_count": 5}

    res2 = await ml_route.reset_signal_weights("tenantX")
    assert res2["update_count"] == 0
    assert "metrics" in res2["weights"]
