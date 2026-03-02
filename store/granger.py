"""
Granger causality test result storage and retrieval logic.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Dict, List

from store.client import redis_get, redis_set
from config import GRANGER_TTL
from store import keys

log = logging.getLogger(__name__)

def _pair_key(cause: str, effect: str) -> str:
    return f"{cause}>>>{effect}"

async def load(tenant_id: str, service: str) -> List[dict]:
    try:
        raw = await redis_get(keys.granger(tenant_id, service))
        if raw:
            return json.loads(raw)
    except Exception as exc:
        log.debug("Granger load failed %s/%s: %s", tenant_id, service, exc)
    return []

async def save_and_merge(tenant_id: str, service: str, fresh_results: list) -> List[dict]:
    cached = await load(tenant_id, service)

    stored: Dict[str, dict] = {
        _pair_key(r["cause_metric"], r["effect_metric"]): r
        for r in cached
    }
    for r in fresh_results:
        pk = _pair_key(r.cause_metric, r.effect_metric)
        existing = stored.get(pk)
        if existing is None or r.strength > existing["strength"]:
            stored[pk] = {
                "cause_metric": r.cause_metric,
                "effect_metric": r.effect_metric,
                "max_lag": r.max_lag,
                "f_statistic": r.f_statistic,
                "p_value": r.p_value,
                "is_causal": r.is_causal,
                "strength": r.strength,
            }

    merged = sorted(stored.values(), key=lambda x: x["strength"], reverse=True)
    try:
        await redis_set(keys.granger(tenant_id, service), json.dumps(merged), ttl=GRANGER_TTL)
    except Exception as exc:
        log.debug("Granger save failed %s/%s: %s", tenant_id, service, exc)
    return merged

async def load_all_services(tenant_id: str, services: List[str]) -> List[dict]:
    per_service = await asyncio.gather(*[load(tenant_id, svc) for svc in services])
    all_results: Dict[str, dict] = {}
    for svc_results in per_service:
        for r in svc_results:
            pk = _pair_key(r["cause_metric"], r["effect_metric"])
            if pk not in all_results or r["strength"] > all_results[pk]["strength"]:
                all_results[pk] = r
    return sorted(all_results.values(), key=lambda x: x["strength"], reverse=True)
