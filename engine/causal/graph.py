"""
Graph structure and logic for representing causal relationships between metrics, allowing for simulation of
interventions and identification of root causes based on a directed acyclic graph (DAG) representation of causality.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
"""

from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass, field

from config import settings
from engine.causal.granger import GrangerResult


@dataclass(frozen=True)
class CausalEdge:
    cause: str
    effect: str
    strength: float
    lag_seconds: float = 0.0


@dataclass
class InterventionResult:
    target: str
    expected_effect_on: dict[str, float] = field(default_factory=dict)
    causal_path: list[str] = field(default_factory=list)
    total_effect: float = 0.0


class CausalGraph:
    def __init__(self) -> None:
        self._edges: list[CausalEdge] = []
        self._forward: dict[str, list[CausalEdge]] = defaultdict(list)
        self._reverse: dict[str, set[str]] = defaultdict(set)

    def add_edge(self, cause: str, effect: str, strength: float, lag_seconds: float = 0.0) -> None:
        edge = CausalEdge(cause=cause, effect=effect, strength=strength, lag_seconds=lag_seconds)
        self._edges.append(edge)
        self._forward[cause].append(edge)
        self._reverse[effect].add(cause)

    def from_granger_results(self, results: list[GrangerResult]) -> None:
        for r in results:
            if r.is_causal:
                self.add_edge(r.cause_metric, r.effect_metric, r.strength)

    def topological_sort(self) -> list[str]:
        nodes = self.all_nodes()
        in_degree: dict[str, int] = {n: 0 for n in nodes}
        for node in self._forward:
            for edge in self._forward[node]:
                in_degree[edge.effect] = in_degree.get(edge.effect, 0) + 1

        queue = deque(n for n in nodes if in_degree.get(n, 0) == 0)
        order: list[str] = []
        while queue:
            node = queue.popleft()
            order.append(node)
            for edge in self._forward.get(node, []):
                in_degree[edge.effect] -= 1
                if in_degree[edge.effect] == 0:
                    queue.append(edge.effect)

        return order

    def root_causes(self) -> list[str]:
        all_effects = {e.effect for e in self._edges}
        all_causes = set(self._forward)
        return sorted(all_causes - all_effects)

    def simulate_intervention(self, target: str, max_depth: int | None = None) -> InterventionResult:
        if max_depth is None:
            max_depth = settings.causal_graph_max_depth
        effects: dict[str, float] = {}
        path: list[str] = []
        queue: deque[tuple[str, float, int]] = deque([(target, 1.0, 0)])
        seen: set[str] = {target}

        while queue:
            node, cumulative_strength, depth = queue.popleft()
            if depth >= max_depth:
                continue
            for edge in self._forward.get(node, []):
                effect_strength = cumulative_strength * edge.strength
                if edge.effect not in seen:
                    seen.add(edge.effect)
                    path.append(edge.effect)
                effects[edge.effect] = round(
                    max(effects.get(edge.effect, 0.0), effect_strength),
                    settings.causal_round_precision,
                )
                queue.append((edge.effect, effect_strength, depth + 1))

        return InterventionResult(
            target=target,
            expected_effect_on=effects,
            causal_path=path,
            total_effect=round(sum(effects.values()), settings.causal_round_precision),
        )

    def find_common_causes(self, node_a: str, node_b: str) -> list[str]:
        def ancestors(node: str) -> set[str]:
            seen: set[str] = set()
            q: deque[str] = deque([node])
            while q:
                n = q.popleft()
                for parent in self._reverse.get(n, set()):
                    if parent not in seen:
                        seen.add(parent)
                        q.append(parent)
            return seen

        return sorted(ancestors(node_a) & ancestors(node_b))

    def all_nodes(self) -> set[str]:
        return set(self._forward) | {e.effect for e in self._edges}
