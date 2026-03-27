"""Shared request helpers for datasource connectors."""

from __future__ import annotations

from typing import Protocol

import httpx

from datasources.helpers import fetch_json
from datasources.types import JSONDict, QueryParams


class _BackendConnector(Protocol):
    base_url: str
    timeout: int
    client: httpx.AsyncClient

    def request_headers(self) -> dict[str, str]:
        ...


async def query_backend_json(
    connector: _BackendConnector,
    *,
    path: str,
    params: QueryParams,
    invalid_msg: str,
    timeout_msg: str,
    unavailable_msg: str,
) -> JSONDict:
    headers_getter = getattr(connector, "request_headers", None)
    if callable(headers_getter):
        headers = headers_getter()
    else:
        legacy_headers_getter = getattr(connector, "_headers", None)
        headers = legacy_headers_getter() if callable(legacy_headers_getter) else {}

    return await fetch_json(
        f"{connector.base_url}{path}",
        params=params,
        headers=headers,
        timeout=connector.timeout,
        client=connector.client,
        invalid_msg=invalid_msg,
        timeout_msg=timeout_msg,
        unavailable_msg=unavailable_msg,
    )
