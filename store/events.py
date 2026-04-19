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
from typing import TypedDict, cast

from config import EVENTS_TTL
from engine.events.models import DeploymentEvent
from store import keys
from store.client import redis_delete, redis_lrange, redis_rpush

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
    coerced: StoredEvent | None = None
    if isinstance(value, dict):
        service = value.get("service")
        version = value.get("version")
        author = value.get("author", "")
        environment = value.get("environment", "production")
        source = value.get("source", "unknown")
        metadata = value.get("metadata", {})
        has_required_fields = isinstance(service, str) and isinstance(version, str)
        has_valid_meta = (
            isinstance(author, str)
            and isinstance(environment, str)
            and isinstance(source, str)
            and isinstance(metadata, dict)
            and all(isinstance(key, str) and isinstance(item, str) for key, item in metadata.items())
        )
        if has_required_fields and has_valid_meta:
            try:
                timestamp = _coerce_float(value.get("timestamp"))
            except (TypeError, ValueError):
                timestamp = None
            if timestamp is not None:
                coerced = cast(
                    StoredEvent,
                    {
                    "service": service,
                    "timestamp": timestamp,
                    "version": version,
                    "author": author,
                    "environment": environment,
                    "source": source,
                    "metadata": metadata,
                    },
                )
    return coerced


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


async def load(tenant_id: str) -> list[StoredEvent]:
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
