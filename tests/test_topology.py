"""
Test Suite for Topology Analysis.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
"""

from engine.topology.graph import BlastRadius, DependencyGraph


def test_dependency_graph():
    g = DependencyGraph()
    g.add_call("a", "b")
    g.add_call("b", "c")
    g.add_call("c", "a")
    assert "b" in g._forward["a"]
    br = g.blast_radius("a", max_depth=2)
    assert isinstance(br, BlastRadius)
    assert "b" in br.affected_downstream
    roots = g.find_upstream_roots("c")
    assert isinstance(roots, list)
    path = g.critical_path("a", "c")
    assert path and path[0] == "a" and path[-1] == "c"
    allsvcs = g.all_services()
    assert allsvcs >= {"a", "b", "c"}


def test_from_spans():
    g = DependencyGraph()
    spans = [
        {
            "rootServiceName": "a",
            "spanSets": [
                {
                    "attributes": [
                        {"key": "service.name", "value": {"stringValue": "a"}},
                        {"key": "peer.service", "value": {"stringValue": "b"}},
                    ]
                },
            ],
        },
        {
            "rootServiceName": "b",
            "spanSets": [
                {
                    "attributes": [
                        {"key": "service.name", "value": {"stringValue": "b"}},
                        {"key": "peer.service", "value": {"stringValue": "d"}},
                    ]
                },
            ],
        },
    ]
    g.from_spans(spans)
    assert "b" in g._forward["a"]
