from typing import Any, Dict, Optional

import httpx

from config import DATASOURCE_TIMEOUT, HEALTH_PATH
from connectors.common import query_backend_json
from datasources.base import MetricsConnector
from datasources.helpers import fetch_text
from datasources.retry import retry


class MimirConnector(MetricsConnector):
    health_path = HEALTH_PATH

    def __init__(
        self,
        base_url: str,
        tenant_id: str,
        timeout: int = DATASOURCE_TIMEOUT,
        headers: Optional[Dict[str, str]] = None,
    ):
        super().__init__(tenant_id, base_url, timeout, headers)

    @retry(attempts=3, delay=0.5, backoff=2.0, exceptions=(httpx.RequestError, httpx.TimeoutException))
    async def scrape(self) -> str:
        url = f"{self.base_url}/metrics"
        return await fetch_text(
            url,
            headers=self._headers(),
            timeout=self.timeout,
            client=self.client,
            invalid_msg="Mimir scrape failed",
            timeout_msg="Mimir scrape timed out",
            unavailable_msg="Cannot reach Mimir at",
        )

    @retry(attempts=3, delay=0.5, backoff=2.0, exceptions=(httpx.RequestError, httpx.TimeoutException))
    async def query_range(
        self,
        query: str,
        start: int,
        end: int,
        step: str,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {"query": query, "start": start, "end": end, "step": step}
        return await query_backend_json(
            self,
            path="/prometheus/api/v1/query_range",
            params=params,
            invalid_msg="Mimir query failed",
            timeout_msg="Mimir query timed out",
            unavailable_msg="Cannot reach Mimir at",
        )
