"""
Datasource factory tests focused on validating that connector timeouts are correctly passed through from the factory to all underlying connectors, ensuring consistent timeout behavior across all data source interactions. This includes testing that the configured timeout value is correctly applied to Loki, Mimir, Tempo, and VictoriaMetrics connectors when they are instantiated by the factory.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
"""
from __future__ import annotations

from types import SimpleNamespace

from config import (
    LOGS_BACKEND_LOKI,
    METRICS_BACKEND_MIMIR,
    METRICS_BACKEND_VICTORIAMETRICS,
    TRACES_BACKEND_TEMPO,
)
from datasources.factory import DataSourceFactory


def test_factory_passes_connector_timeout_to_all_connectors(monkeypatch):
    captured: dict[str, int] = {}

    def fake_loki(url, tenant_id, timeout=None, headers=None):
        captured["logs"] = timeout
        return ("loki", timeout)

    def fake_mimir(url, tenant_id, timeout=None, headers=None):
        captured["metrics"] = timeout
        return ("mimir", timeout)

    def fake_tempo(url, tenant_id, timeout=None, headers=None):
        captured["traces"] = timeout
        return ("tempo", timeout)

    monkeypatch.setattr("datasources.factory.LokiConnector", fake_loki)
    monkeypatch.setattr("datasources.factory.MimirConnector", fake_mimir)
    monkeypatch.setattr("datasources.factory.TempoConnector", fake_tempo)

    cfg = SimpleNamespace(
        logs_backend=LOGS_BACKEND_LOKI,
        metrics_backend=METRICS_BACKEND_MIMIR,
        traces_backend=TRACES_BACKEND_TEMPO,
        loki_url="http://loki",
        mimir_url="http://mimir",
        victoriametrics_url="http://victoria",
        tempo_url="http://tempo",
        connector_timeout=42,
    )

    assert DataSourceFactory.create_logs(cfg, "tenant")[1] == 42
    assert DataSourceFactory.create_metrics(cfg, "tenant")[1] == 42
    assert DataSourceFactory.create_traces(cfg, "tenant")[1] == 42
    assert captured == {"logs": 42, "metrics": 42, "traces": 42}


def test_factory_passes_timeout_to_victoria_connector(monkeypatch):
    captured: dict[str, int] = {}

    def fake_victoria(url, tenant_id, timeout=None, headers=None):
        captured["victoria"] = timeout
        return ("victoria", timeout)

    monkeypatch.setattr("datasources.factory.VictoriaMetricsConnector", fake_victoria)

    cfg = SimpleNamespace(
        metrics_backend=METRICS_BACKEND_VICTORIAMETRICS,
        victoriametrics_url="http://victoria",
        connector_timeout=13,
    )
    assert DataSourceFactory.create_metrics(cfg, "tenant")[1] == 13
    assert captured["victoria"] == 13
