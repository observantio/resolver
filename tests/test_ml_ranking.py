"""
Tests for RCA cause ranking (RandomForest + per-row attribution).

Copyright (c) 2026 Stefan Kumarasinghe.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. See http://www.apache.org/licenses/LICENSE-2.0 for details.
"""

from __future__ import annotations

import engine.ml.ranking as ranking
from engine.enums import RcaCategory, Severity
from engine.rca.hypothesis import RootCause


def _cause(conf: float, hypothesis: str = "h") -> RootCause:
    return RootCause(
        hypothesis=hypothesis,
        confidence=conf,
        severity=Severity.MEDIUM,
        category=RcaCategory.UNKNOWN,
        evidence=[],
        contributing_signals=["metric:cpu"],
        affected_services=["a"],
    )


def test_rank_empty():
    assert ranking.rank([]) == []


def test_rank_import_error_falls_back(monkeypatch):
    def boom(_name):
        raise ImportError("no sklearn")

    monkeypatch.setattr(ranking, "import_module", boom)
    causes = [_cause(0.5), _cause(0.6)]
    out = ranking.rank(causes)
    assert len(out) == 2
    assert set(out[0].feature_importance.keys()) == set(ranking._FEATURE_NAMES)


def test_rank_four_causes_uses_rf_and_per_row_importance_differs():
    causes = [_cause(0.9, "a"), _cause(0.7, "b"), _cause(0.5, "c"), _cause(0.2, "d")]
    out = ranking.rank(causes)
    assert len(out) == 4
    imps = [r.feature_importance for r in out]
    assert imps[0] != imps[-1]


def test_ranking_pseudo_labels_balanced():
    causes = [_cause(float(i) / 10) for i in range(4)]
    labels = ranking._ranking_pseudo_labels(causes)
    assert set(labels) == {0, 1}


def test_per_row_shares_sum_to_one():
    import numpy as np

    row = np.array([0.5, 0.3, 0.2, 0.1, 0.0, 0.0, 0.0, 0.0, 0.0])
    g = np.ones(9) / 9
    d = ranking._per_row_importance_share(row, g)
    assert abs(sum(d.values()) - 1.0) < 1e-6


def test_rank_single_class_pseudo_labels_falls_back(monkeypatch):
    monkeypatch.setattr(ranking, "_ranking_pseudo_labels", lambda _: [0, 0, 0, 0])
    causes = [_cause(0.5 + i * 0.01, f"h{i}") for i in range(4)]
    out = ranking.rank(causes)
    assert len(out) == 4
    assert all(isinstance(r.feature_importance, dict) for r in out)
