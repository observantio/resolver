import re
from typing import Any, Dict, Optional

import httpx

from config import DATASOURCE_TIMEOUT, HEALTH_PATH
from connectors.common import query_backend_json
from datasources.base import LogsConnector
from datasources.retry import retry


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
        params: Dict[str, Any] = {"query": self._normalize_query(query), "start": start, "end": end}
        if limit is not None:
            params["limit"] = limit
        return await query_backend_json(
            self,
            path="/loki/api/v1/query_range",
            params=params,
            invalid_msg="Loki query failed",
            timeout_msg="Loki query timed out",
            unavailable_msg="Cannot reach Loki at",
        )
