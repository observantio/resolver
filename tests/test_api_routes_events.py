"""
Test API routes for deployment events, ensuring correct handling of tenant context, request validation, and integration
with the event registry.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
"""

import pytest

from api.routes import events as events_route
from api.requests import DeploymentEventRequest


class DummyRegistry:
    def __init__(self):
        self.events = []

    async def register_event(self, tenant_id, event):
        self.events.append((tenant_id, event))


@pytest.mark.asyncio
async def test_register_deployment_with_tenant_in_body(monkeypatch):
    dummy = DummyRegistry()
    monkeypatch.setattr(events_route, "get_registry", lambda: dummy)
    req = DeploymentEventRequest(tenant_id="t1", service="s", timestamp=123, version="v1")
    res = await events_route.register_deployment(req)
    assert res["status"] == "registered"
    assert dummy.events[0][0] == "t1"


@pytest.mark.asyncio
async def test_register_deployment_with_query_param(monkeypatch):
    dummy = DummyRegistry()
    monkeypatch.setattr(events_route, "get_registry", lambda: dummy)
    req = DeploymentEventRequest(tenant_id="t1", service="s", timestamp=123, version="v1")
    # result not needed; only side effect on dummy registry is asserted
    _ = await events_route.register_deployment(req, tenant_id="t2")
    assert dummy.events[0][0] == "t2"


@pytest.mark.asyncio
async def test_register_deployment_missing_tenant(monkeypatch):
    dummy = DummyRegistry()
    monkeypatch.setattr(events_route, "get_registry", lambda: dummy)

    class FakeReq:
        service = "s"
        timestamp = 1
        version = "v1"
        author = ""
        environment = "production"
        source = "api"
        metadata = {}

    with pytest.raises(Exception):
        await events_route.register_deployment(FakeReq())
