"""
Factory for creating data source connectors based on configuration.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
"""

from connectors.loki import LokiConnector
from connectors.mimir import MimirConnector
from connectors.tempo import TempoConnector
from connectors.victoria import VictoriaMetricsConnector

class DataSourceFactory:

    @staticmethod
    def create_logs(config, tenant_id):
        from config import LOGS_BACKEND_LOKI
        if config.logs_backend == LOGS_BACKEND_LOKI:
            return LokiConnector(config.loki_url, tenant_id, timeout=config.connector_timeout)
        raise ValueError("Unsupported logs backend")

    @staticmethod
    def create_metrics(config, tenant_id):
        from config import METRICS_BACKEND_MIMIR, METRICS_BACKEND_VICTORIAMETRICS

        if config.metrics_backend == METRICS_BACKEND_MIMIR:
            return MimirConnector(config.mimir_url, tenant_id, timeout=config.connector_timeout)
        if config.metrics_backend == METRICS_BACKEND_VICTORIAMETRICS:
            return VictoriaMetricsConnector(config.victoriametrics_url, tenant_id, timeout=config.connector_timeout)
        raise ValueError("Unsupported metrics backend")

    @staticmethod
    def create_traces(config, tenant_id):
        from config import TRACES_BACKEND_TEMPO
        if config.traces_backend == TRACES_BACKEND_TEMPO:
            return TempoConnector(config.tempo_url, tenant_id, timeout=config.connector_timeout)
        raise ValueError("Unsupported traces backend")
