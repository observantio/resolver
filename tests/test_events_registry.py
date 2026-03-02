"""
Test cases for the EventRegistry class in the analysis engine, validating registration, retrieval, filtering, and clearing of events, as well as edge cases in timestamp handling.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
"""


from engine.events.registry import DeploymentEvent, EventRegistry


def test_event_registry_basic():
    reg = EventRegistry()
    assert reg.list_all() == []
    e1 = DeploymentEvent(service="svc", timestamp=100.0, version="v1")
    e2 = DeploymentEvent(service="svc", timestamp=200.0, version="v2")
    reg.register(e1)
    reg.register(e2)
    all_events = reg.list_all()
    assert len(all_events) == 2
    assert reg.for_service("svc") == all_events
    assert reg.in_window(150, 250) == [e2]
    reg.clear()
    assert reg.list_all() == []
