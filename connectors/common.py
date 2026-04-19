"""
Shared request helpers for datasource connectors.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

import httpx

from datasources.helpers import FetchErrorMessages, FetchRequestOptions, fetch_json
from datasources.types import JSONDict, QueryParams


class _BackendConnector(Protocol):
    base_url: str
    timeout: int
    client: httpx.AsyncClient

    def request_headers(self) -> dict[str, str]: ...


@dataclass(frozen=True)
class BackendErrorMessages:
    invalid: str
    timeout: str
    unavailable: str


async def query_backend_json(
    connector: _BackendConnector,
    path: str,
    params: QueryParams,
    messages: BackendErrorMessages | None = None,
) -> JSONDict:
    resolved_messages = messages or BackendErrorMessages(invalid="", timeout="", unavailable="")

    headers_getter = getattr(connector, "request_headers", None)
    if callable(headers_getter):
        headers = headers_getter()
    else:
        fallback_headers_getter = getattr(connector, "_headers", None)
        headers = fallback_headers_getter() if callable(fallback_headers_getter) else {}

    return await fetch_json(
        f"{connector.base_url}{path}",
        options=FetchRequestOptions(
            params=params,
            headers=headers,
            timeout=connector.timeout,
            client=connector.client,
        ),
        messages=_to_fetch_messages(resolved_messages),
    )


def _to_fetch_messages(messages: BackendErrorMessages) -> FetchErrorMessages:
    return FetchErrorMessages(
        invalid_msg=messages.invalid,
        timeout_msg=messages.timeout,
        unavailable_msg=messages.unavailable,
    )
