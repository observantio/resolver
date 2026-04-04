"""
Grouping logic for deduplication of anomalies, providing functionality to cluster similar anomalies based on temporal
proximity and optionally by metric name, to reduce noise and improve signal quality for downstream analysis.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Generic, List, TypeVar

from api.responses import MetricAnomaly
from config import settings

T = TypeVar("T")


@dataclass
class AnomalyGroup(Generic[T]):
    representative: T
    members: List[T] = field(default_factory=list)
    count: int = 1


def group_metric_anomalies(
    anomalies: List[MetricAnomaly],
    time_window: float | None = None,
    by_metric: bool = True,
) -> List[AnomalyGroup[MetricAnomaly]]:
    if time_window is None:
        time_window = settings.dedup_time_window
    if not anomalies:
        return []

    sorted_a = sorted(anomalies, key=lambda a: a.timestamp)
    groups: List[AnomalyGroup[MetricAnomaly]] = []
    current = AnomalyGroup(representative=sorted_a[0], members=[sorted_a[0]])

    for a in sorted_a[1:]:
        rep = current.representative
        same_metric = (not by_metric) or (a.metric_name == rep.metric_name)
        close_in_time = abs(a.timestamp - rep.timestamp) <= time_window

        if same_metric and close_in_time:
            current.members.append(a)
            current.count += 1
            if a.severity.weight() > rep.severity.weight():
                current.representative = a
        else:
            groups.append(current)
            current = AnomalyGroup(representative=a, members=[a])

    groups.append(current)
    return groups
