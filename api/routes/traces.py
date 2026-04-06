"""
Trace analysis routes for detecting anomalous latency patterns and service degradations in distributed systems.

Copyright (c) 2026 Stefan Kumarasinghe Licensed under the Apache License, Version 2.0 (the "License"); you may not use
this file except in compliance with the License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
"""

from __future__ import annotations

from fastapi import APIRouter, Depends

from api.requests import TraceRequest
from api.responses import ServiceLatency
from api.routes.common import get_provider, safe_call
from api.routes.exception import handle_exceptions
from datasources.types import TraceFilters
from engine import traces
from services.security_service import enforce_request_tenant, require_permission_dependency

router = APIRouter(tags=["Traces"])


@router.post(
    "/anomalies/traces",
    response_model=list[ServiceLatency],
    dependencies=[Depends(require_permission_dependency("read:rca"))],
)
@handle_exceptions
async def trace_anomalies(req: TraceRequest) -> list[ServiceLatency]:
    req = enforce_request_tenant(req)
    filters: TraceFilters = {}
    if req.service:
        filters["service.name"] = req.service
    raw = await safe_call(get_provider(req.tenant_id).query_traces(filters=filters, start=req.start, end=req.end))
    return traces.analyze(raw, req.apdex_threshold_ms)
