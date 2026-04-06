"""
ML routes for adaptive signal weighting based on user feedback.

Copyright (c) 2026 Stefan Kumarasinghe Licensed under the Apache License, Version 2.0 (the "License"); you may not use
this file except in compliance with the License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from api.routes.common import safe_call
from api.routes.exception import handle_exceptions
from custom_types.json import JSONDict
from engine.enums import Signal
from engine.registry import get_registry
from services.security_service import get_context_tenant, require_permission_dependency

router = APIRouter(tags=["ML"])


@router.post(
    "/ml/weights/feedback",
    summary="Submit signal correctness feedback",
    dependencies=[Depends(require_permission_dependency("create:rca"))],
)
@handle_exceptions
async def signal_feedback(tenant_id: str, signal: str, was_correct: bool) -> JSONDict:
    tenant_id = get_context_tenant(tenant_id)
    try:
        sig = Signal(signal)
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown signal '{signal}'. Valid values: {[s.value for s in Signal]}",
        ) from exc
    state = await safe_call(get_registry().update_weight(tenant_id, sig, was_correct), 500)
    return {"updated_weights": state.weights_serializable, "update_count": state.update_count}


@router.get(
    "/ml/weights",
    summary="Current adaptive signal weights for a tenant",
    dependencies=[Depends(require_permission_dependency("read:rca"))],
)
@handle_exceptions
async def get_signal_weights(tenant_id: str) -> JSONDict:
    tenant_id = get_context_tenant(tenant_id)
    state = await safe_call(get_registry().get_state(tenant_id), 500)
    return {"weights": state.weights_serializable, "update_count": state.update_count}


@router.post(
    "/ml/weights/reset",
    summary="Reset adaptive weights to defaults for a tenant",
    dependencies=[Depends(require_permission_dependency("delete:rca"))],
)
@handle_exceptions
async def reset_signal_weights(tenant_id: str) -> JSONDict:
    tenant_id = get_context_tenant(tenant_id)
    state = await safe_call(get_registry().reset_weights(tenant_id), 500)
    return {"weights": state.weights_serializable, "update_count": state.update_count}
