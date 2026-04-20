"""
Test Suite Conftest.

Copyright (c) 2026 Stefan Kumarasinghe.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. See http://www.apache.org/licenses/LICENSE-2.0 for details.
"""

import atexit
import os
import shutil
import sys
import tempfile
from pathlib import Path

import pytest

os.environ.setdefault("MUTANT_UNDER_TEST", "")

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

_TEST_SQLITE_DIR = Path(tempfile.mkdtemp(prefix="observantio-resolver-tests-"))
_TEST_SQLITE_URL = f"sqlite:///{_TEST_SQLITE_DIR / 'resolver.sqlite3'}"
_TEST_DATABASE_INITIALIZED = False

atexit.register(shutil.rmtree, _TEST_SQLITE_DIR, ignore_errors=True)


def _bootstrap_sqlite_database() -> None:
    global _TEST_DATABASE_INITIALIZED

    use_temp_sqlite = os.getenv("USE_TEMP_SQLITE_TEST_DB", "").strip().lower() in {"1", "true", "yes", "on"}
    database_url = os.getenv("RESOLVER_DATABASE_URL", "").strip()
    if use_temp_sqlite:
        database_url = _TEST_SQLITE_URL
        os.environ["RESOLVER_DATABASE_URL"] = database_url
        os.environ["DATABASE_URL"] = database_url

    if _TEST_DATABASE_INITIALIZED or not database_url.startswith("sqlite"):
        return

    from database import dispose_database, init_database, init_db

    dispose_database()
    init_database(database_url)
    init_db()
    _TEST_DATABASE_INITIALIZED = True


_bootstrap_sqlite_database()

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

    modules = []
    for module_name in ("store.weights", "store.baseline", "store.granger", "store.events"):
        try:
            mod = __import__(module_name, fromlist=["_placeholder"])
        except ImportError:
            continue
        modules.append(mod)

    for mod in modules:
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
