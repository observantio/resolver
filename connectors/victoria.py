from typing import Any, Dict, Optional

import httpx

from config import HEALTH_PATH, DATASOURCE_TIMEOUT
from connectors.common import query_backend_json
from datasources.base import MetricsConnector
from datasources.retry import retry


class VictoriaMetricsConnector(MetricsConnector):
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
            path="/api/v1/query_range",
            params=params,
            invalid_msg="VictoriaMetrics query failed",
            timeout_msg="VictoriaMetrics query timed out",
            unavailable_msg="Cannot reach VictoriaMetrics at",
        )
