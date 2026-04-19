"""
Mimir connector for Metrics queries. This module defines a MimirConnector class that provides methods to scrape metrics 
and query time series data from a Mimir instance. It uses httpx for making asynchronous HTTP requests to the Mimir API 
and includes error handling for common issues such as invalid queries, timeouts, and service unavailability. 
The connector is designed to be used within the resolver service to fetch metric data for analysis and correlation
with other telemetry data.

Copyright (c) 2026 Stefan Kumarasinghe.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. See http://www.apache.org/licenses/LICENSE-2.0 for details.
"""

import httpx

from config import DATASOURCE_TIMEOUT, HEALTH_PATH
from connectors.common import BackendErrorMessages, query_backend_json
from datasources.base import MetricsConnector
from datasources.helpers import FetchErrorMessages, FetchRequestOptions, fetch_text
from datasources.retry import retry
from datasources.types import JSONDict, QueryParams


class MimirConnector(MetricsConnector):
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
    async def scrape(self) -> str:
        url = f"{self.base_url}/metrics"
        return await fetch_text(
            url,
            options=FetchRequestOptions(headers=self._headers(), timeout=self.timeout, client=self.client),
            messages=FetchErrorMessages(
                invalid_msg="Mimir scrape failed",
                timeout_msg="Mimir scrape timed out",
                unavailable_msg="Cannot reach Mimir at",
            ),
        )

    @retry(attempts=3, delay=0.5, backoff=2.0, exceptions=(httpx.RequestError, httpx.TimeoutException))
    async def query_range(
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
        params: QueryParams = {"query": query, "start": start, "end": end, "step": resolved_step}
        messages = BackendErrorMessages(
            invalid="Mimir query failed",
            timeout="Mimir query timed out",
            unavailable="Cannot reach Mimir at",
        )
        return await query_backend_json(
            self,
            "/prometheus/api/v1/query_range",
            params,
            messages=messages,
        )
