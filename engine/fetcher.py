"""
Fetcher Module for Metrics Retrieval and Scrape Fallback.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
"""

from __future__ import annotations

import asyncio
import logging
import re

import httpx

from config import settings
from custom_types.json import JSONDict
from datasources.provider import DataSourceProvider

log = logging.getLogger(__name__)

METRIC_NAME_RE = re.compile(r"[a-zA-Z_:][a-zA-Z0-9_:]*")


def _extract_metric_names(query: str) -> list[str]:
    return METRIC_NAME_RE.findall(query)


async def _scrape_and_fill(
    provider: DataSourceProvider,
    queries: list[str],
    start: int,
    end: int,
) -> list[tuple[str, JSONDict]]:
    metrics_backend = getattr(provider, "metrics", None)
    scrape_func = getattr(metrics_backend, "scrape", None)
    if not callable(scrape_func):
        return []

    try:
        text = await scrape_func()
    except (TimeoutError, httpx.HTTPError, OSError, TypeError, ValueError) as exc:
        log.warning("scrape_and_fill: scrape failed: %s", exc)
        return []

    metrics: dict[str, float] = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split()
        if len(parts) < 2:
            continue
        name = parts[0].split("{", 1)[0]
        try:
            metrics[name] = float(parts[1])
        except ValueError:
            continue

    if not metrics:
        log.debug("scrape_and_fill: scrape returned no parseable metrics")
        return []

    results: list[tuple[str, JSONDict]] = []
    for q in queries:
        candidates = {n for n in _extract_metric_names(q) if n in metrics}
        for name in candidates:
            val = metrics[name]
            results.append(
                (
                    q,
                    {
                        "status": "success",
                        "data": {
                            "result": [
                                {
                                    "metric": {"__name__": name},
                                    "values": [[start, val], [end, val]],
                                }
                            ]
                        },
                    },
                )
            )
            break

    return results


async def fetch_metrics(
    provider: DataSourceProvider,
    queries: list[str],
    start: int,
    end: int,
    *,
    step: str | None = None,
) -> list[tuple[str, JSONDict]]:
    resolved_step = None if step is None else str(step)
    if not resolved_step:
        raise TypeError("step is required")

    max_parallel = max(1, int(settings.analyzer_max_parallel_metric_queries))
    sem = asyncio.Semaphore(max_parallel)

    async def _query(q: str) -> JSONDict:
        async with sem:
            return await provider.query_metrics(query=q, start=start, end=end, step=resolved_step)

    raw = await asyncio.gather(*[_query(q) for q in queries], return_exceptions=True)

    pairs: list[tuple[str, JSONDict]] = []
    any_results = False

    for q, r in zip(queries, raw):
        if isinstance(r, Exception):
            log.warning("fetch_metrics query=%s failed: %s", q, r)
            continue
        if not isinstance(r, dict):
            log.warning("fetch_metrics query=%s returned non-dict response: %s", q, type(r).__name__)
            continue
        data = r.get("data")
        results = data.get("result") if isinstance(data, dict) else []
        cnt = len(results) if isinstance(results, list) else 0
        log.debug("fetch_metrics query=%s series=%d", q, cnt)
        pairs.append((q, r))
        if cnt > 0:
            any_results = True

    if pairs and not any_results:
        log.info("fetch_metrics: all queries returned empty; attempting scrape fallback")
        scraped = await _scrape_and_fill(provider, queries, start, end)
        if scraped:
            return scraped

    return pairs
