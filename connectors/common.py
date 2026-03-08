"""Shared request helpers for datasource connectors."""

from __future__ import annotations

from typing import Any, Dict

from datasources.helpers import fetch_json


async def query_backend_json(
    connector,
    *,
    path: str,
    params: Dict[str, Any],
    invalid_msg: str,
    timeout_msg: str,
    unavailable_msg: str,
) -> Dict[str, Any]:
    return await fetch_json(
        f"{connector.base_url}{path}",
        params=params,
        headers=connector._headers(),
        timeout=connector.timeout,
        client=connector.client,
        invalid_msg=invalid_msg,
        timeout_msg=timeout_msg,
        unavailable_msg=unavailable_msg,
    )
