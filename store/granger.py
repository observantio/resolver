"""
Granger causality test result storage and retrieval logic.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
"""

from __future__ import annotations

import asyncio
import json
import logging
from json import JSONDecodeError
from typing import Dict, List, TypedDict

from config import GRANGER_TTL
from engine.causal.granger import GrangerResult
from store import keys
from store.client import redis_get, redis_set

log = logging.getLogger(__name__)


class GrangerRecord(TypedDict):
    cause_metric: str
    effect_metric: str
    max_lag: int
    f_statistic: float
    p_value: float
    is_causal: bool
    strength: float


def _coerce_int(value: object) -> int:
    if isinstance(value, bool):
        raise TypeError("bool is not a valid integer")
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        return int(value)
    raise TypeError(f"Unsupported integer value: {type(value).__name__}")


def _coerce_float(value: object) -> float:
    if isinstance(value, bool):
        raise TypeError("bool is not a valid float")
    if isinstance(value, (int, float, str)):
        return float(value)
    raise TypeError(f"Unsupported float value: {type(value).__name__}")


def _pair_key(cause: str, effect: str) -> str:
    return f"{cause}>>>{effect}"


def _coerce_record(value: object) -> GrangerRecord | None:
    if not isinstance(value, dict):
        return None
    cause_metric = value.get("cause_metric")
    effect_metric = value.get("effect_metric")
    if not isinstance(cause_metric, str) or not isinstance(effect_metric, str):
        return None
    max_lag_raw: object = value.get("max_lag")
    f_statistic_raw: object = value.get("f_statistic")
    p_value_raw: object = value.get("p_value")
    strength_raw: object = value.get("strength")
    try:
        max_lag = _coerce_int(max_lag_raw)
        f_statistic = _coerce_float(f_statistic_raw)
        p_value = _coerce_float(p_value_raw)
        strength = _coerce_float(strength_raw)
    except (TypeError, ValueError):
        return None
    is_causal = value.get("is_causal")
    if not isinstance(is_causal, bool):
        return None
    return {
        "cause_metric": cause_metric,
        "effect_metric": effect_metric,
        "max_lag": max_lag,
        "f_statistic": f_statistic,
        "p_value": p_value,
        "is_causal": is_causal,
        "strength": strength,
    }


async def load(tenant_id: str, service: str) -> List[GrangerRecord]:
    try:
        raw = await redis_get(keys.granger(tenant_id, service))
        if raw:
            payload = json.loads(raw)
            if not isinstance(payload, list):
                return []
            records: list[GrangerRecord] = []
            for item in payload:
                record = _coerce_record(item)
                if record is not None:
                    records.append(record)
            return records
    except (TypeError, ValueError, JSONDecodeError) as exc:
        log.debug("Granger load failed %s/%s: %s", tenant_id, service, exc)
    return []


async def save_and_merge(
    tenant_id: str,
    service: str,
    fresh_results: list[GrangerResult],
) -> List[GrangerRecord]:
    cached = await load(tenant_id, service)

    stored: Dict[str, GrangerRecord] = {_pair_key(r["cause_metric"], r["effect_metric"]): r for r in cached}
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
    except (TypeError, ValueError) as exc:
        log.debug("Granger save failed %s/%s: %s", tenant_id, service, exc)
    return merged


async def load_all_services(tenant_id: str, services: List[str]) -> List[GrangerRecord]:
    per_service = await asyncio.gather(*[load(tenant_id, svc) for svc in services])
    all_results: Dict[str, GrangerRecord] = {}
    for svc_results in per_service:
        for r in svc_results:
            pk = _pair_key(r["cause_metric"], r["effect_metric"])
            if pk not in all_results or r["strength"] > all_results[pk]["strength"]:
                all_results[pk] = r
    return sorted(all_results.values(), key=lambda x: x["strength"], reverse=True)
