"""
Test cases for Fetcher logic in the analysis engine, including data retrieval, caching behavior, and error handling for
different signal types.

Copyright (c) 2026 Stefan Kumarasinghe.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. See http://www.apache.org/licenses/LICENSE-2.0 for details.
"""

import pytest

from engine.fetcher import fetch_metrics


class DummyProvider:
    def __init__(self, results):
        self._results = results

        class Metrics:
            async def scrape(self):
                return ""

        self.metrics = Metrics()

    async def query_metrics(self, query, start, end, step):
        if query == "bad":
            raise ValueError("oops")
        return {"query": query, "start": start}


@pytest.mark.asyncio
async def test_fetch_metrics_filters_exceptions():
    provider = DummyProvider(None)
    queries = ["a", "bad", "c"]
    res = await fetch_metrics(provider, queries, 0, 1, step="15s")
    assert isinstance(res, list)
    assert all(isinstance(r, tuple) and isinstance(r[1], dict) for r in res)
    assert len(res) == 2
    assert res[0][0] == "a"
