"""
Registry for deployment events, allowing for recording and querying of deployment-related information such as service
name, timestamp, version, author, environment, source, and additional metadata, to facilitate correlation with observed
anomalies and support root cause analysis.

Copyright (c) 2026 Stefan Kumarasinghe.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. See http://www.apache.org/licenses/LICENSE-2.0 for details.
"""

from __future__ import annotations

from engine.events.models import DeploymentEvent

__all__ = ["DeploymentEvent", "EventRegistry"]


class EventRegistry:
    def __init__(self) -> None:
        self._events: list[DeploymentEvent] = []

    def register(self, event: DeploymentEvent) -> None:
        self._events.append(event)

    def in_window(self, start: float, end: float) -> list[DeploymentEvent]:
        return [e for e in self._events if start <= e.timestamp <= end]

    def for_service(self, service: str) -> list[DeploymentEvent]:
        return [e for e in self._events if e.service == service]

    def list_all(self) -> list[DeploymentEvent]:
        return list(self._events)

    def clear(self) -> None:
        self._events.clear()
