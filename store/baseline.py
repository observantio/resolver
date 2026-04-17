"""
Baseline computation and persistence logic.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
"""

from __future__ import annotations

from importlib import import_module
import json
import logging
from json import JSONDecodeError
from typing import TYPE_CHECKING, Any, cast

from config import BASELINE_TTL, BLEND_ALPHA
from store import keys
from store.client import redis_get, redis_set

if TYPE_CHECKING:
    from engine.baseline.compute import Baseline

log = logging.getLogger(__name__)


def _load_compute_module() -> Any:
    return import_module("engine.baseline.compute")


def __getattr__(name: str) -> Any:
    if name in {"Baseline", "compute"}:
        value = getattr(_load_compute_module(), name)
        globals()[name] = value
        return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def _to_json(b: Baseline) -> str:
    return json.dumps(
        {
            "mean": b.mean,
            "std": b.std,
            "lower": b.lower,
            "upper": b.upper,
            "seasonal_mean": b.seasonal_mean,
            "sample_count": b.sample_count,
        }
    )


def _from_json(data: str) -> Baseline:
    baseline_cls = _baseline_cls()
    d = json.loads(data)
    return baseline_cls(
        mean=d["mean"],
        std=d["std"],
        lower=d["lower"],
        upper=d["upper"],
        seasonal_mean=d.get("seasonal_mean"),
        sample_count=d.get("sample_count", 0),
    )


def _blend(cached: Baseline, fresh: Baseline) -> Baseline:
    baseline_cls = _baseline_cls()
    a = 1.0 - BLEND_ALPHA
    blended_mean = a * cached.mean + BLEND_ALPHA * fresh.mean
    blended_std = a * cached.std + BLEND_ALPHA * fresh.std
    return baseline_cls(
        mean=round(blended_mean, 6),
        std=round(max(blended_std, 1e-9), 6),
        lower=round(blended_mean - 3 * blended_std, 6),
        upper=round(blended_mean + 3 * blended_std, 6),
        seasonal_mean=fresh.seasonal_mean or cached.seasonal_mean,
        sample_count=cached.sample_count + fresh.sample_count,
    )


async def load(tenant_id: str, metric_name: str) -> Baseline | None:
    try:
        raw = await redis_get(keys.baseline(tenant_id, metric_name))
        if raw:
            return _from_json(raw)
    except (TypeError, ValueError, KeyError, JSONDecodeError) as exc:
        log.debug("Baseline load failed %s/%s: %s", tenant_id, metric_name, exc)
    return None


async def save(tenant_id: str, metric_name: str, baseline: Baseline) -> None:
    try:
        await redis_set(keys.baseline(tenant_id, metric_name), _to_json(baseline), ttl=BASELINE_TTL)
    except (TypeError, ValueError) as exc:
        log.debug("Baseline save failed %s/%s: %s", tenant_id, metric_name, exc)


async def compute_and_persist(
    tenant_id: str,
    metric_name: str,
    ts: list[float],
    vals: list[float],
    z_threshold: float = 3.0,
) -> Baseline:
    fresh = _compute(ts, vals, z_threshold=z_threshold)
    cached = await load(tenant_id, metric_name)

    result = _blend(cached, fresh) if cached and cached.sample_count >= 20 else fresh
    await save(tenant_id, metric_name, result)
    return result


def _baseline_cls() -> type[Any]:
    baseline_cls = globals().get("Baseline")
    if baseline_cls is None:
        baseline_cls = __getattr__("Baseline")
    return cast(type[Any], baseline_cls)


def _compute(ts: list[float], vals: list[float], z_threshold: float) -> Baseline:
    compute_fn = globals().get("compute")
    if compute_fn is None:
        compute_fn = __getattr__("compute")
    return compute_fn(ts, vals, z_threshold=z_threshold)
