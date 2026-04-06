"""
Weights storage and retrieval logic.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
"""

from __future__ import annotations

import json
import logging
from json import JSONDecodeError
from typing import TypedDict

from config import WEIGHTS_TTL
from custom_types.json import is_json_object
from store import keys
from store.client import redis_delete, redis_get, redis_set

log = logging.getLogger(__name__)


class StoredWeights(TypedDict):
    weights: dict[str, object]
    update_count: int


async def load(tenant_id: str) -> StoredWeights | None:
    try:
        raw = await redis_get(keys.weights(tenant_id))
        if raw:
            payload = json.loads(raw)
            if not is_json_object(payload):
                return None
            weights = payload.get("weights")
            if not isinstance(weights, dict):
                return None
            update_count = payload.get("update_count", 0)
            if isinstance(update_count, bool):
                update_count = int(update_count)
            elif isinstance(update_count, int):
                pass
            elif isinstance(update_count, float):
                update_count = int(update_count)
            elif isinstance(update_count, str):
                try:
                    update_count = int(update_count)
                except ValueError:
                    update_count = 0
            else:
                update_count = 0
            return {"weights": weights, "update_count": max(0, update_count)}
    except (TypeError, ValueError, JSONDecodeError) as exc:
        log.debug("Weights load failed %s: %s", tenant_id, exc)
    return None


async def save(tenant_id: str, weight_map: dict[str, float], update_count: int) -> None:
    payload = {"weights": weight_map, "update_count": update_count}
    try:
        await redis_set(keys.weights(tenant_id), json.dumps(payload), ttl=WEIGHTS_TTL)
    except (TypeError, ValueError) as exc:
        log.debug("Weights save failed %s: %s", tenant_id, exc)


async def delete(tenant_id: str) -> None:
    await redis_delete(keys.weights(tenant_id))
