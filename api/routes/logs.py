"""
Log analysis routes for detecting anomalous patterns and bursts in log data.

Copyright (c) 2026 Stefan Kumarasinghe Licensed under the Apache License, Version 2.0 (the "License"); you may not use
this file except in compliance with the License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
"""

from __future__ import annotations

from typing import List

from fastapi import APIRouter, Depends

from api.requests import LogRequest
from api.responses import LogBurst, LogPattern
from api.routes.common import get_provider, safe_call, to_nanoseconds
from api.routes.exception import handle_exceptions
from engine import logs
from services.security_service import enforce_request_tenant, require_permission_dependency

router = APIRouter(tags=["Logs"])


@router.post(
    "/anomalies/logs/patterns",
    response_model=List[LogPattern],
    dependencies=[Depends(require_permission_dependency("read:rca"))],
)
@handle_exceptions
async def log_patterns(req: LogRequest) -> List[LogPattern]:
    req = enforce_request_tenant(req)
    raw = await safe_call(
        get_provider(req.tenant_id).query_logs(
            query=req.query, start=to_nanoseconds(req.start), end=to_nanoseconds(req.end)
        )
    )
    return logs.analyze(raw)


@router.post(
    "/anomalies/logs/bursts",
    response_model=List[LogBurst],
    dependencies=[Depends(require_permission_dependency("read:rca"))],
)
@handle_exceptions
async def log_bursts(req: LogRequest) -> List[LogBurst]:
    req = enforce_request_tenant(req)
    raw = await safe_call(
        get_provider(req.tenant_id).query_logs(
            query=req.query, start=to_nanoseconds(req.start), end=to_nanoseconds(req.end)
        )
    )
    return logs.detect_bursts(raw)
