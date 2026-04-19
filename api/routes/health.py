"""
Health check route to verify service and store connectivity.

Copyright (c) 2026 Stefan Kumarasinghe.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use
this file except in compliance with the License. See http://www.apache.org/licenses/LICENSE-2.0 for details.
"""

from __future__ import annotations

from fastapi import APIRouter

from api.routes.exception import handle_exceptions
from custom_types.json import JSONDict
from store.client import get_redis, is_using_fallback

router = APIRouter(tags=["Health"])


@router.get(
    "/health",
    summary="Service health probe",
    description="Checks resolver health and reports the active store backend.",
    response_description="Current resolver health state and store backend in use.",
)
@handle_exceptions
async def health() -> JSONDict:
    await get_redis()
    return {
        "status": "ok",
        "store": "fallback" if is_using_fallback() else "redis",
    }
