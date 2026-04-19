"""
Shared helper functions for data source connectors.

Copyright (c) 2026 Stefan Kumarasinghe.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. See http://www.apache.org/licenses/LICENSE-2.0 for details.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, cast

import httpx

from datasources.exceptions import DataSourceUnavailable, InvalidQuery, QueryTimeout
from datasources.types import JSONDict, QueryParams


@dataclass(frozen=True)
class FetchRequestOptions:
    params: QueryParams | None = None
    headers: dict[str, str] | None = None
    timeout: int = 30
    client: Any | None = None


@dataclass(frozen=True)
class FetchErrorMessages:
    invalid_msg: str
    timeout_msg: str
    unavailable_msg: str


_DEFAULT_JSON_MESSAGES = FetchErrorMessages(
    invalid_msg="query failed",
    timeout_msg="query timed out",
    unavailable_msg="Cannot reach data source at",
)

_DEFAULT_TEXT_MESSAGES = FetchErrorMessages(
    invalid_msg="request failed",
    timeout_msg="request timed out",
    unavailable_msg="Cannot reach data source at",
)


def _coerce_fetch_options(options: FetchRequestOptions | None) -> FetchRequestOptions:
    base = options or FetchRequestOptions()
    timeout = int(cast(int | str | bytes | bytearray, base.timeout))
    client_raw = base.client
    client = client_raw if hasattr(client_raw, "get") else base.client

    return FetchRequestOptions(
        params=base.params,
        headers=base.headers,
        timeout=timeout,
        client=client,
    )


def _coerce_error_messages(
    messages: FetchErrorMessages | None,
    defaults: FetchErrorMessages,
) -> FetchErrorMessages:
    base = messages or defaults
    return FetchErrorMessages(
        invalid_msg=str(base.invalid_msg),
        timeout_msg=str(base.timeout_msg),
        unavailable_msg=str(base.unavailable_msg),
    )


async def fetch_json(
    url: str,
    options: FetchRequestOptions | None = None,
    messages: FetchErrorMessages | None = None,
) -> JSONDict:
    parsed_options = _coerce_fetch_options(options)
    parsed_messages = _coerce_error_messages(messages, _DEFAULT_JSON_MESSAGES)

    try:
        if parsed_options.client is None:
            async with httpx.AsyncClient(timeout=parsed_options.timeout) as owned_client:
                resp = await owned_client.get(url, params=parsed_options.params, headers=parsed_options.headers)
        else:
            resp = await parsed_options.client.get(url, params=parsed_options.params, headers=parsed_options.headers)
        resp.raise_for_status()
        payload = resp.json()
        return payload if isinstance(payload, dict) else {}
    except httpx.HTTPStatusError as e:
        raise InvalidQuery(f"{parsed_messages.invalid_msg} [{e.response.status_code}]: {e.response.text}") from e
    except httpx.TimeoutException as e:
        raise QueryTimeout(parsed_messages.timeout_msg) from e
    except httpx.RequestError as e:
        raise DataSourceUnavailable(f"{parsed_messages.unavailable_msg} {url}") from e


async def fetch_text(
    url: str,
    options: FetchRequestOptions | None = None,
    messages: FetchErrorMessages | None = None,
) -> str:
    parsed_options = _coerce_fetch_options(options)
    parsed_messages = _coerce_error_messages(messages, _DEFAULT_TEXT_MESSAGES)

    try:
        if parsed_options.client is None:
            async with httpx.AsyncClient(timeout=parsed_options.timeout) as owned_client:
                resp = await owned_client.get(url, headers=parsed_options.headers)
        else:
            resp = await parsed_options.client.get(url, headers=parsed_options.headers)
        resp.raise_for_status()
        return resp.text
    except httpx.HTTPStatusError as e:
        raise InvalidQuery(f"{parsed_messages.invalid_msg} [{e.response.status_code}]: {e.response.text}") from e
    except httpx.TimeoutException as e:
        raise QueryTimeout(parsed_messages.timeout_msg) from e
    except httpx.RequestError as e:
        raise DataSourceUnavailable(f"{parsed_messages.unavailable_msg} {url}") from e
