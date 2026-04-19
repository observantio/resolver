"""
Ready tests for the API service, focused on validating that route permissions are correctly wired and enforced.

Copyright (c) 2026 Stefan Kumarasinghe.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. See http://www.apache.org/licenses/LICENSE-2.0 for details.
"""

from __future__ import annotations

import json

import pytest

import main as app_main
from config import LOGS_BACKEND_LOKI, METRICS_BACKEND_MIMIR, TRACES_BACKEND_TEMPO


@pytest.mark.asyncio
async def test_ready_endpoint_returns_503_with_backend_details_when_not_ready():
    app_main._BACKEND_READY = False
    app_main._BACKEND_STATUS = {"mimir": "failed: timeout", "tempo": "ready"}
    response = await app_main.ready()
    payload = json.loads(response.body.decode("utf-8"))
    assert response.status_code == 503
    assert payload["ready"] is False
    assert payload["backends"]["mimir"].startswith("failed:")


@pytest.mark.asyncio
async def test_wait_for_all_bg_sets_backend_ready_false_on_partial_failure(monkeypatch):
    class DummySettings:
        logs_backend = LOGS_BACKEND_LOKI
        metrics_backend = METRICS_BACKEND_MIMIR
        traces_backend = TRACES_BACKEND_TEMPO
        loki_url = "http://loki"
        mimir_url = "http://mimir"
        tempo_url = "http://tempo"
        startup_timeout = 1

    async def fake_wait_for(name, url, timeout, headers=None, accept_status=(200,)):
        if name == METRICS_BACKEND_MIMIR:
            raise RuntimeError("mimir down")
        return None

    monkeypatch.setattr(app_main, "wait_for", fake_wait_for)
    app_main._BACKEND_READY = True
    app_main._BACKEND_STATUS = {}

    await app_main._wait_for_all_bg(DummySettings(), "tenant-a")

    assert app_main._BACKEND_READY is False
    assert app_main._BACKEND_STATUS[METRICS_BACKEND_MIMIR].startswith("failed:")
