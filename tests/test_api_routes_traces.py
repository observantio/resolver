"""
Api route tests for trace anomaly detection paths, focused on validating that the trace query route correctly handles cases where no service filters are provided, ensuring that it does not apply any default service filters and allows the provider to process the request with an empty filter set as intended.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
"""

from __future__ import annotations

import pytest

from api.requests import TraceRequest
from api.routes import traces as traces_route


class DummyProvider:
    def __init__(self) -> None:
        self.filters = None

    async def query_traces(self, filters, start, end, limit=None):
        self.filters = dict(filters)
        return {"traces": []}


@pytest.mark.asyncio
async def test_trace_route_does_not_force_default_service_filter(monkeypatch):
    provider = DummyProvider()
    monkeypatch.setattr(traces_route, "get_provider", lambda tid: provider)
    monkeypatch.setattr(traces_route.traces, "analyze", lambda raw, apdex: [])

    req = TraceRequest(tenant_id="t1", start=1, end=2)
    rows = await traces_route.trace_anomalies(req)

    assert rows == []
    assert provider.filters == {}
