"""
Provider for data source connectors to query logs, metrics, and traces based on tenant configuration.

Copyright (c) 2026 Stefan Kumarasinghe.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. See http://www.apache.org/licenses/LICENSE-2.0 for details.
"""

from .base import LogsConnector, MetricsConnector, TracesConnector
from .data_config import DataSourceSettings
from .factory import DataSourceFactory
from .types import JSONDict, TraceFilters


class DataSourceProvider:
    def __init__(self, tenant_id: str, settings: DataSourceSettings) -> None:
        self.tenant_id = tenant_id
        self.settings = settings
        self.logs: LogsConnector
        self.metrics: MetricsConnector
        self.traces: TracesConnector
        self.logs = DataSourceFactory.create_logs(settings, tenant_id)
        self.metrics = DataSourceFactory.create_metrics(settings, tenant_id)
        self.traces = DataSourceFactory.create_traces(settings, tenant_id)

    async def query_logs(
        self,
        query: str,
        start: int,
        end: int,
        *,
        limit: int | None = None,
    ) -> JSONDict:
        resolved_limit = _coerce_optional_int(limit)
        return await self.logs.query_range(query=query, start=start, end=end, limit=resolved_limit)

    async def query_metrics(
        self,
        query: str,
        start: int,
        end: int,
        *,
        step: str | None = None,
    ) -> JSONDict:
        resolved_step = None if step is None else str(step)
        if not resolved_step:
            raise TypeError("step is required")
        return await self.metrics.query_range(query=query, start=start, end=end, step=resolved_step)

    async def query_traces(
        self,
        filters: TraceFilters,
        start: int,
        end: int,
        *,
        limit: int | None = None,
    ) -> JSONDict:
        resolved_limit = _coerce_optional_int(limit)
        return await self.traces.query_range(filters=filters, start=start, end=end, limit=resolved_limit)

    async def aclose(self) -> None:
        await self.logs.aclose()
        await self.metrics.aclose()
        await self.traces.aclose()


def _coerce_optional_int(value: object | None) -> int | None:
    if value is None:
        return None
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return None
