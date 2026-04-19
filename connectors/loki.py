"""
Loki connector for Log and LogQL queries. This module defines a LokiConnector
class that provides methods to query logs and log labels from a Loki instance.
It uses httpx for asynchronous HTTP requests to the Loki API and includes error
handling for invalid queries, timeouts, and service unavailability. The connector
is designed to fetch log data for analysis and correlation with other telemetry
data.

Copyright (c) 2026 Stefan Kumarasinghe.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. See http://www.apache.org/licenses/LICENSE-2.0 for details.
"""

import re

import httpx

from config import DATASOURCE_TIMEOUT, HEALTH_PATH
from connectors.common import BackendErrorMessages, query_backend_json
from datasources.base import LogsConnector
from datasources.retry import retry
from datasources.types import JSONDict


class LokiConnector(LogsConnector):
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

    @staticmethod
    def _normalize_query(query: str) -> str:
        q = (query or "").strip()
        if not q or q == "{}":
            return '{service=~".+"}'

        q = re.sub(r'=~"\.\*"', '=~".+"', q)
        return q

    @retry(attempts=3, delay=0.5, backoff=2.0, exceptions=(httpx.RequestError, httpx.TimeoutException))
    async def query_range(
        self,
        query: str,
        start: int,
        end: int,
        *,
        limit: int | None = None,
    ) -> JSONDict:
        params: dict[str, str | int | float | bool] = {
            "query": self._normalize_query(query),
            "start": start,
            "end": end,
        }
        if limit is not None:
            params["limit"] = limit
        messages = BackendErrorMessages(
            invalid="Loki query failed",
            timeout="Loki query timed out",
            unavailable="Cannot reach Loki at",
        )
        return await query_backend_json(
            self,
            "/loki/api/v1/query_range",
            params,
            messages=messages,
        )
