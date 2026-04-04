"""
Analyze service implementation that runs the core analysis engine with tenant-aware data providers.

Copyright (c) 2026 Stefan Kumarasinghe Licensed under the Apache License, Version 2.0 (the "License"); you may not use
this file except in compliance with the License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
"""

from __future__ import annotations

from api.requests import AnalyzeRequest
from api.responses import AnalysisReport
from datasources.provider import DataSourceProvider
from engine.analyzer import run
from services.analysis_config_service import PreparedAnalysisRequest, analysis_config_service
from services.security_service import enforce_request_tenant


def get_provider(tenant_id: str) -> DataSourceProvider:
    from api.routes.common import get_provider as route_get_provider

    return route_get_provider(tenant_id)


async def run_analysis(
    req: AnalyzeRequest,
    *,
    explicit_fields: set[str] | None = None,
    prepared: PreparedAnalysisRequest | None = None,
) -> AnalysisReport:
    tenant_req = enforce_request_tenant(req)
    active_prepared = prepared or analysis_config_service.prepare_request(
        tenant_req,
        explicit_fields=explicit_fields,
    )
    async with analysis_config_service.apply_runtime_overrides(active_prepared):
        return await run(get_provider(active_prepared.request.tenant_id), active_prepared.request)
