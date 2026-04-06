import httpx

from config import DATASOURCE_TIMEOUT, HEALTH_PATH
from connectors.common import query_backend_json
from datasources.base import TracesConnector
from datasources.retry import retry
from datasources.types import JSONDict, TraceFilters


class TempoConnector(TracesConnector):
    health_path = HEALTH_PATH

    def __init__(
        self,
        base_url: str,
        tenant_id: str,
        timeout: int = DATASOURCE_TIMEOUT,
        headers: dict[str, str] | None = None,
    ) -> None:
        super().__init__(tenant_id, base_url, timeout, headers)

    @retry(attempts=3, delay=0.5, backoff=2.0, exceptions=(httpx.RequestError, httpx.TimeoutException))
    async def query_range(
        self,
        filters: TraceFilters,
        start: int,
        end: int,
        limit: int | None = None,
    ) -> JSONDict:
        params: dict[str, str | int | float | bool] = {"start": start, "end": end, **filters}
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
