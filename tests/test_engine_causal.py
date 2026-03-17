"""
Test engine causal analysis logic, including correlation of anomalies, log bursts, and service latency, as well as edge cases in timestamp handling and relevance filtering.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
"""

from engine.causal.graph import CausalGraph, InterventionResult
from engine.causal.bayesian import score as bayesian_score
from engine.causal.granger import granger_pair_analysis, granger_multiple_pairs, GrangerResult


def test_bayesian_score_consistency():
    results = bayesian_score(True, False, False, False, False)
    assert abs(sum(r.posterior for r in results) - 1.0) < 1e-6
    assert results[0].category.name == "deployment"


def test_causal_graph_basic():
    g = CausalGraph()
    g.add_edge("a", "b", 0.5)
    g.add_edge("b", "c", 0.4)
    order = g.topological_sort()
    assert order[0] == "a"
    assert g.root_causes() == ["a"]
    inter = g.simulate_intervention("a", max_depth=2)
    assert isinstance(inter, InterventionResult)
    assert "b" in inter.expected_effect_on
    assert g.find_common_causes("b", "c") == ["a"]


def test_granger_pair_and_all():
    import numpy as np

    rng = np.random.default_rng(1234)
    cause = rng.standard_normal(200).tolist()
    effect_arr = np.roll(cause, 1) + rng.standard_normal(200) * 0.1
    effect_arr[0] = 0.0
    effect = effect_arr.tolist()

    res = granger_pair_analysis("c", cause, "e", effect, max_lag=1)
    assert isinstance(res, GrangerResult)
    assert res.cause_metric == "c"
    assert res.effect_metric == "e"

    allr = granger_multiple_pairs({"c": cause, "e": effect}, max_lag=1)
    assert allr
    assert any(result.cause_metric == "c" and result.effect_metric == "e" for result in allr)
