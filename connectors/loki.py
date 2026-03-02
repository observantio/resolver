"""
Loki connector implementation for querying logs from a Loki instance.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
"""

import httpx
import re
from typing import Any, Dict, Optional

from datasources.retry import retry

from datasources.base import LogsConnector
from datasources.helpers import fetch_json
from config import HEALTH_PATH, DATASOURCE_TIMEOUT
from datasources.exceptions import DataSourceUnavailable, InvalidQuery, QueryTimeout

class LokiConnector(LogsConnector):
    health_path = HEALTH_PATH

    def __init__(
        self,
        base_url: str,
        tenant_id: str,
        timeout: int = DATASOURCE_TIMEOUT,
        headers: Optional[Dict[str, str]] = None,
    ):
        super().__init__(tenant_id, base_url, timeout, headers)

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
        limit: Optional[int] = None,
    ) -> Dict[str, Any]:
        url = f"{self.base_url}/loki/api/v1/query_range"
        params: Dict[str, Any] = {"query": self._normalize_query(query), "start": start, "end": end}
        if limit is not None:
            params["limit"] = limit
        return await fetch_json(
            url,
            params=params,
            headers=self._headers(),
            timeout=self.timeout,
            client=self.client,
            invalid_msg="Loki query failed",
            timeout_msg="Loki query timed out",
            unavailable_msg="Cannot reach Loki at",
        )
