"""
Shared helper functions for data source connectors.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
"""

from __future__ import annotations

from typing import Optional
import httpx
from datasources.exceptions import DataSourceUnavailable, InvalidQuery, QueryTimeout
from datasources.types import JSONDict, QueryParams


async def fetch_json(
    url: str,
    params: Optional[QueryParams] = None,
    headers: Optional[dict[str, str]] = None,
    timeout: int = 30,
    client: Optional[httpx.AsyncClient] = None,
    invalid_msg: str = "query failed",
    timeout_msg: str = "query timed out",
    unavailable_msg: str = "Cannot reach data source at",
) -> JSONDict:
    try:
        if client is None:
            async with httpx.AsyncClient(timeout=timeout) as owned_client:
                resp = await owned_client.get(url, params=params, headers=headers)
        else:
            resp = await client.get(url, params=params, headers=headers)
        resp.raise_for_status()
        payload = resp.json()
        return payload if isinstance(payload, dict) else {}
    except httpx.HTTPStatusError as e:
        raise InvalidQuery(f"{invalid_msg} [{e.response.status_code}]: {e.response.text}") from e
    except httpx.TimeoutException as e:
        raise QueryTimeout(timeout_msg) from e
    except httpx.RequestError as e:
        raise DataSourceUnavailable(f"{unavailable_msg} {url}") from e


async def fetch_text(
    url: str,
    headers: Optional[dict[str, str]] = None,
    timeout: int = 30,
    client: Optional[httpx.AsyncClient] = None,
    invalid_msg: str = "request failed",
    timeout_msg: str = "request timed out",
    unavailable_msg: str = "Cannot reach data source at",
) -> str:
    try:
        if client is None:
            async with httpx.AsyncClient(timeout=timeout) as owned_client:
                resp = await owned_client.get(url, headers=headers)
        else:
            resp = await client.get(url, headers=headers)
        resp.raise_for_status()
        return resp.text
    except httpx.HTTPStatusError as e:
        raise InvalidQuery(f"{invalid_msg} [{e.response.status_code}]: {e.response.text}") from e
    except httpx.TimeoutException as e:
        raise QueryTimeout(timeout_msg) from e
    except httpx.RequestError as e:
        raise DataSourceUnavailable(f"{unavailable_msg} {url}") from e
