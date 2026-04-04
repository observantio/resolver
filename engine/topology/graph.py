"""
Graph representation of service dependencies, with methods for blast radius analysis, upstream root finding, and
critical path detection.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
"""

from __future__ import annotations

from collections import defaultdict, deque
from config import settings
from dataclasses import dataclass
from typing import Dict, List, Set


@dataclass(frozen=True)
class BlastRadius:
    root_service: str
    affected_downstream: List[str]
    depth: int


class DependencyGraph:
    def __init__(self) -> None:
        self._forward: Dict[str, Set[str]] = defaultdict(set)
        self._reverse: Dict[str, Set[str]] = defaultdict(set)

    def add_call(self, caller: str, callee: str) -> None:
        if caller == callee or not caller or not callee:
            return
        self._forward[caller].add(callee)
        self._reverse[callee].add(caller)

    def from_spans(self, raw: object) -> None:
        traces = raw.get("traces", []) if isinstance(raw, dict) else raw
        if not isinstance(traces, list):
            return

        def _attributes(raw_attributes: object) -> list[dict[str, object]]:
            if not isinstance(raw_attributes, list):
                return []
            return [item for item in raw_attributes if isinstance(item, dict)]

        def _spans(raw_spans: object) -> list[dict[str, object]]:
            if not isinstance(raw_spans, list):
                return []
            return [item for item in raw_spans if isinstance(item, dict)]

        def _attr_value(attributes: list[dict[str, object]], key: str) -> str:
            for attr in attributes:
                if attr.get("key") != key:
                    continue
                value = attr.get("value") or {}
                if not isinstance(value, dict):
                    continue
                text = (value.get("stringValue") or value.get("value") or "").strip()
                if text:
                    return text
            return ""

        for trace in traces:
            if not isinstance(trace, dict):
                continue
            root = trace.get("rootServiceName")
            span_sets: list[dict[str, object]] = []
            raw_sets = trace.get("spanSets")
            if isinstance(raw_sets, list):
                span_sets.extend(s for s in raw_sets if isinstance(s, dict))

            single_set = trace.get("spanSet")
            if isinstance(single_set, dict):
                span_sets.append(single_set)

            for span_set in span_sets:
                span_attributes = _attributes(span_set.get("attributes"))
                svc = _attr_value(span_attributes, "service.name")
                peer = _attr_value(span_attributes, "peer.service") or _attr_value(
                    span_attributes,
                    "db.name",
                )
                for span in _spans(span_set.get("spans")):
                    attrs = _attributes(span.get("attributes"))
                    if not svc:
                        svc = _attr_value(attrs, "service.name")
                    if not peer:
                        peer = _attr_value(attrs, "peer.service") or _attr_value(attrs, "db.name")
                if svc and peer:
                    self.add_call(svc, peer)
                elif root and peer:
                    self.add_call(root, peer)

    def blast_radius(self, root: str, max_depth: int | None = None) -> BlastRadius:
        if max_depth is None:
            max_depth = settings.topology_max_depth
        affected: List[str] = []
        seen: Set[str] = {root}
        queue: deque[tuple[str, int]] = deque([(root, 0)])

        while queue:
            node, depth = queue.popleft()
            if depth >= max_depth:
                continue
            for neighbor in self._forward.get(node, set()):
                if neighbor not in seen:
                    seen.add(neighbor)
                    affected.append(neighbor)
                    queue.append((neighbor, depth + 1))

        return BlastRadius(root_service=root, affected_downstream=affected, depth=max_depth)

    def find_upstream_roots(self, service: str) -> List[str]:
        roots: List[str] = []
        seen: Set[str] = set()
        queue: deque[str] = deque([service])

        while queue:
            node = queue.popleft()
            if node in seen:
                continue
            seen.add(node)
            callers = self._reverse.get(node, set())
            if not callers:
                roots.append(node)
            else:
                queue.extend(callers)

        return roots

    def critical_path(self, source: str, target: str) -> List[str]:
        if source == target:
            return [source]

        queue: deque[List[str]] = deque([[source]])
        seen: Set[str] = set()

        while queue:
            path = queue.popleft()
            node = path[-1]
            if node == target:
                return path
            if node in seen:
                continue
            seen.add(node)
            for neighbor in self._forward.get(node, set()):
                queue.append(path + [neighbor])

        return []

    def all_services(self) -> Set[str]:
        return set(self._forward) | set(self._reverse)
