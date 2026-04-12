"""
Test Suite Conftest.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
"""

import os
import sys

import pytest

os.environ.setdefault("MUTANT_UNDER_TEST", "")

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from store.client import _fallback  # noqa: E402


@pytest.fixture(autouse=True)
def clear_fallback(monkeypatch):
    import store.client as client

    _fallback.clear()
    client._REDIS_CLIENT = None
    client._USING_FALLBACK = False
    client._RETRY_AFTER_MONOTONIC = 0.0

    async def fake_get(key: str):
        return _fallback.get(key)

    async def fake_set(key: str, value: str, ttl=None):
        _fallback[key] = value

    async def fake_delete(key: str):
        _fallback.pop(key, None)

    monkeypatch.setattr(client, "redis_get", fake_get)
    monkeypatch.setattr(client, "redis_set", fake_set)
    monkeypatch.setattr(client, "redis_delete", fake_delete)

    import store.baseline as bstore
    import store.events as estore
    import store.granger as gstore
    import store.weights as wstore

    for mod in (wstore, bstore, gstore, estore):
        for name in ("redis_get", "redis_set", "redis_delete"):
            if hasattr(mod, name):
                monkeypatch.setattr(mod, name, locals()[f"fake_{name.split('_')[1]}"])

    yield

    _fallback.clear()
    client._REDIS_CLIENT = None
    client._USING_FALLBACK = False
    client._RETRY_AFTER_MONOTONIC = 0.0


def pytest_ignore_collect(collection_path, path=None, config=None):
    text = str(collection_path)
    if os.path.sep + "engine" + os.path.sep in text:
        return True
