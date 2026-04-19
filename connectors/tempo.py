"""
Tempo connector for topology and trace queries. This module defines a
TempoConnector class that provides methods to query traces from a Tempo
instance. It uses httpx for asynchronous HTTP requests to the Tempo API and
includes error handling for invalid queries, timeouts, and service
unavailability. The connector is designed to fetch trace data for analysis and
correlation with other telemetry data.

Copyright (c) 2026 Stefan Kumarasinghe.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. See http://www.apache.org/licenses/LICENSE-2.0 for details.
"""

import httpx

from config import DATASOURCE_TIMEOUT, HEALTH_PATH
from connectors.common import BackendErrorMessages, query_backend_json
from datasources.base import TracesConnector
from datasources.retry import retry
from datasources.types import JSONDict, TraceFilters


class TempoConnector(TracesConnector):
    health_path = HEALTH_PATH

    def __init__(
        self,
        base_url: str,
        tenant_id: str,
        *,
        timeout: int = DATASOURCE_TIMEOUT,
        headers: dict[str, str] | None = None,
    ) -> None:
        super().__init__(tenant_id, base_url, timeout=timeout, headers=headers)

    @retry(attempts=3, delay=0.5, backoff=2.0, exceptions=(httpx.RequestError, httpx.TimeoutException))
    async def query_range(
        self,
        filters: TraceFilters,
        start: int,
        end: int,
        *,
        limit: int | None = None,
    ) -> JSONDict:
        params: dict[str, str | int | float | bool] = {"start": start, "end": end, **filters}
        if limit is not None:
            params["limit"] = limit
        messages = BackendErrorMessages(
            invalid="Tempo query failed",
            timeout="Tempo query timed out",
            unavailable="Cannot reach Tempo at",
        )
        return await query_backend_json(
            self,
            "/api/search",
            params,
            messages=messages,
        )
