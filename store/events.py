"""
Event storage and retrieval logic.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
"""

from __future__ import annotations

import json
import logging
from json import JSONDecodeError
from typing import List, TypedDict

from engine.events.models import DeploymentEvent
from store.client import redis_lrange, redis_rpush, redis_delete
from config import EVENTS_TTL
from store import keys

log = logging.getLogger(__name__)

_MAX_EVENTS = 500


class StoredEvent(TypedDict):
    service: str
    timestamp: float
    version: str
    author: str
    environment: str
    source: str
    metadata: dict[str, str]


def _coerce_float(value: object) -> float:
    if isinstance(value, bool):
        raise TypeError("bool is not a valid float")
    if isinstance(value, (int, float, str)):
        return float(value)
    raise TypeError(f"Unsupported float value: {type(value).__name__}")


def _coerce_event(value: object) -> StoredEvent | None:
    if not isinstance(value, dict):
        return None
    service = value.get("service")
    version = value.get("version")
    author = value.get("author", "")
    environment = value.get("environment", "production")
    source = value.get("source", "unknown")
    metadata = value.get("metadata", {})
    if not isinstance(service, str) or not isinstance(version, str):
        return None
    if not isinstance(author, str) or not isinstance(environment, str) or not isinstance(source, str):
        return None
    if not isinstance(metadata, dict) or not all(
        isinstance(key, str) and isinstance(item, str) for key, item in metadata.items()
    ):
        return None
    timestamp_raw: object = value.get("timestamp")
    try:
        timestamp = _coerce_float(timestamp_raw)
    except (TypeError, ValueError):
        return None
    return {
        "service": service,
        "timestamp": timestamp,
        "version": version,
        "author": author,
        "environment": environment,
        "source": source,
        "metadata": metadata,
    }


def _serialise(event: DeploymentEvent) -> str:
    return json.dumps(
        {
            "service": event.service,
            "timestamp": event.timestamp,
            "version": event.version,
            "author": event.author,
            "environment": event.environment,
            "source": event.source,
            "metadata": dict(event.metadata),
        }
    )


async def load(tenant_id: str) -> List[StoredEvent]:
    try:
        items = await redis_lrange(keys.events(tenant_id))
        events: list[StoredEvent] = []
        for item in items:
            parsed = json.loads(item)
            event = _coerce_event(parsed)
            if event is not None:
                events.append(event)
        return events
    except (TypeError, ValueError, JSONDecodeError) as exc:
        log.debug("Events load failed %s: %s", tenant_id, exc)
    return []


async def append(tenant_id: str, event: DeploymentEvent) -> None:
    try:
        await redis_rpush(keys.events(tenant_id), _serialise(event), ttl=EVENTS_TTL, max_len=_MAX_EVENTS)
    except (TypeError, ValueError) as exc:
        log.debug("Events append failed %s: %s", tenant_id, exc)


async def clear(tenant_id: str) -> None:
    await redis_delete(keys.events(tenant_id))
