"""
Base connectors and shared utilities for data sources.

Copyright (c) 2026 Stefan Kumarasinghe.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. See http://www.apache.org/licenses/LICENSE-2.0 for details.
"""

from abc import ABC, abstractmethod

import httpx

from datasources.types import JSONDict, TraceFilters


class BaseConnector(ABC):
    health_path: str = ""

    def __init__(
        self,
        tenant_id: str,
        base_url: str,
        *,
        timeout: int = 30,
        headers: dict[str, str] | None = None,
    ):
        self.tenant_id = tenant_id
        self.base_url = str(base_url).rstrip("/")
        self.timeout = timeout
        self.headers = headers or {}
        self.client = httpx.AsyncClient(timeout=self.timeout)

    @property
    def health_url(self) -> str:
        if not self.health_path:
            raise NotImplementedError("connector must define health_path")
        return f"{self.base_url}{self.health_path}"

    def _headers(self) -> dict[str, str]:
        return {**self.headers, "X-Scope-OrgID": self.tenant_id}

    def request_headers(self) -> dict[str, str]:
        return self._headers()

    async def aclose(self) -> None:
        await self.client.aclose()


class LogsConnector(BaseConnector):
    @abstractmethod
    async def query_range(
        self,
        query: str,
        start: int,
        end: int,
        *,
        limit: int | None = None,
    ) -> JSONDict: ...


class MetricsConnector(BaseConnector):
    @abstractmethod
    async def query_range(
        self,
        query: str,
        start: int,
        end: int,
        *,
        step: str,
    ) -> JSONDict: ...


class TracesConnector(BaseConnector):
    @abstractmethod
    async def query_range(
        self,
        filters: TraceFilters,
        start: int,
        end: int,
        *,
        limit: int | None = None,
    ) -> JSONDict: ...
