"""
Test cases for the retry logic in the analysis engine, including handling of transient errors, backoff strategies, and
edge cases in retry conditions.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
"""

import asyncio

import pytest

import datasources.retry as retry_module
from datasources.retry import retry


@pytest.mark.asyncio
async def test_retry_async_success_after_failure():
    calls = []

    @retry(attempts=3, delay=0.01, backoff=1, exceptions=(ValueError,))
    async def flaky(x):
        calls.append(x)
        if len(calls) < 2:
            raise ValueError("temporary")
        return x * 2

    result = await flaky(5)
    assert result == 10
    assert len(calls) == 2


@pytest.mark.asyncio
async def test_retry_async_applies_backoff_sleep(monkeypatch):
    calls = []
    sleeps = []

    async def fake_sleep(delay):
        sleeps.append(delay)

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)

    @retry(attempts=3, delay=0.25, backoff=2.0, exceptions=(ValueError,))
    async def flaky():
        calls.append("x")
        if len(calls) < 2:
            raise ValueError("temporary")
        return "ok"

    assert await flaky() == "ok"
    assert sleeps == [0.25]


def test_retry_sync_success_after_failure():
    calls = []

    @retry(attempts=4, delay=0.01, backoff=1, exceptions=(ValueError,))
    def flaky(x):
        calls.append(x)
        if len(calls) < 3:
            raise ValueError("oops")
        return x + 1

    result = flaky(7)
    assert result == 8
    assert len(calls) == 3


def test_retry_exhausted():
    @retry(attempts=2, delay=0.01, backoff=1, exceptions=(ValueError,))
    def always_fail():
        raise ValueError("nope")

    with pytest.raises(ValueError):
        always_fail()
