"""
Event registration routes for recording deployment events and other relevant occurrences that can be used for RCA
correlation.

Copyright (c) 2026 Stefan Kumarasinghe Licensed under the Apache License, Version 2.0 (the "License"); you may not use
this file except in compliance with the License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
"""

from __future__ import annotations

from typing import Dict, List

from fastapi import APIRouter, Depends, HTTPException

from api.requests import DeploymentEventRequest
from api.routes.exception import handle_exceptions
from custom_types.json import JSONDict
from engine.events.models import DeploymentEvent
from engine.registry import get_registry
from services.security_service import enforce_request_tenant, get_context_tenant, require_permission_dependency

router = APIRouter(tags=["Events"])


@router.post(
    "/events/deployment",
    summary="Register a deployment event for RCA correlation",
    dependencies=[Depends(require_permission_dependency("create:rca"))],
)
@handle_exceptions
async def register_deployment(req: DeploymentEventRequest, tenant_id: str | None = None) -> Dict[str, str]:
    req = enforce_request_tenant(req)
    tid = get_context_tenant(tenant_id or req.tenant_id)
    if not isinstance(tid, str) or not tid.strip():
        raise HTTPException(status_code=400, detail="tenant_id must be a non-empty string")

    await get_registry().register_event(
        tid,
        DeploymentEvent.model_validate(req.model_dump()),
    )
    return {"status": "registered", "service": req.service, "version": req.version}


@router.get(
    "/events/deployments",
    summary="List registered deployment events for a tenant",
    dependencies=[Depends(require_permission_dependency("read:rca"))],
)
@handle_exceptions
async def list_deployments(tenant_id: str) -> List[JSONDict]:
    return [
        {
            "service": item["service"],
            "timestamp": item["timestamp"],
            "version": item["version"],
            "author": item["author"],
            "environment": item["environment"],
            "source": item["source"],
            "metadata": item["metadata"],
        }
        for item in await get_registry().get_events(get_context_tenant(tenant_id))
    ]


@router.delete(
    "/events/deployments",
    summary="Clear all deployment events for a tenant",
    dependencies=[Depends(require_permission_dependency("delete:rca"))],
)
@handle_exceptions
async def clear_deployments(tenant_id: str) -> Dict[str, str]:
    resolved_tenant = get_context_tenant(tenant_id)
    await get_registry().clear_events(resolved_tenant)
    return {"status": "cleared", "tenant_id": resolved_tenant}
