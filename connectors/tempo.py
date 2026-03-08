from typing import Any, Dict, Optional

import httpx

from config import DATASOURCE_TIMEOUT, HEALTH_PATH
from connectors.common import query_backend_json
from datasources.base import TracesConnector
from datasources.retry import retry


class TempoConnector(TracesConnector):
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
        filters: Dict[str, Any],
        start: int,
        end: int,
        limit: Optional[int] = None,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {"start": start, "end": end, **filters}
        if limit is not None:
            params["limit"] = limit
        return await query_backend_json(
            self,
            path="/api/search",
            params=params,
            invalid_msg="Tempo query failed",
            timeout_msg="Tempo query timed out",
            unavailable_msg="Cannot reach Tempo at",
        )
