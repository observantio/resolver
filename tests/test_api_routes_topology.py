"""
Test API routes for topology analysis endpoints, validating request handling, response formatting, and integration with
the analysis engine.

Copyright (c) 2026 Stefan Kumarasinghe.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. See http://www.apache.org/licenses/LICENSE-2.0 for details.
"""

import pytest

from api.requests import TopologyRequest
from api.routes import topology as topology_route


class DummyProvider:
    async def query_traces(self, filters, start, end, limit=None):
        return {
            "traces": [
                {
                    "rootServiceName": "checkout",
                    "spanSet": {
                        "spans": [
                            {
                                "attributes": [
                                    {"key": "service.name", "value": {"stringValue": "checkout"}},
                                    {"key": "peer.service", "value": {"stringValue": "payments"}},
                                ]
                            }
                        ]
                    },
                },
                {
                    "rootServiceName": "payments",
                    "spanSets": [
                        {
                            "attributes": [
                                {"key": "service.name", "value": {"stringValue": "payments"}},
                                {"key": "peer.service", "value": {"stringValue": "db"}},
                            ]
                        }
                    ],
                },
            ]
        }


@pytest.mark.asyncio
async def test_topology_blast_radius_supports_dict_trace_payload(monkeypatch):
    dummy = DummyProvider()
    monkeypatch.setattr(topology_route, "get_provider", lambda tid: dummy)

    req = TopologyRequest(
        tenant_id="t1",
        start=1,
        end=100,
        root_service="checkout",
        max_depth=3,
    )
    result = await topology_route.blast_radius(req)

    assert "payments" in result["affected_downstream"]
    assert "checkout" in result["all_services"]
    assert "payments" in result["all_services"]
    assert result["critical_paths"]["payments"] == ["checkout", "payments"]
