"""
Event storage and retrieval logic.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
"""

from __future__ import annotations

import json
import logging
from typing import List

from store.client import redis_lrange, redis_rpush, redis_delete
from config import EVENTS_TTL
from store import keys

log = logging.getLogger(__name__)

_MAX_EVENTS = 500

def _serialise(event) -> str:
    return json.dumps({
        "service": event.service,
        "timestamp": event.timestamp,
        "version": event.version,
        "author": event.author,
        "environment": event.environment,
        "source": event.source,
        "metadata": dict(event.metadata),
    })

async def load(tenant_id: str) -> List[dict]:
    try:
        items = await redis_lrange(keys.events(tenant_id))
        return [json.loads(item) for item in items]
    except Exception as exc:
        log.debug("Events load failed %s: %s", tenant_id, exc)
    return []

async def append(tenant_id: str, event) -> None:
    try:
        await redis_rpush(keys.events(tenant_id), _serialise(event), ttl=EVENTS_TTL, max_len=_MAX_EVENTS)
    except Exception as exc:
        log.debug("Events append failed %s: %s", tenant_id, exc)

async def clear(tenant_id: str) -> None:
    try:
        await redis_delete(keys.events(tenant_id))
    except Exception as exc:
        log.debug("Events clear failed %s: %s", tenant_id, exc)