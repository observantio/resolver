"""
Health check route to verify service and store connectivity.

Copyright (c) 2026 Stefan Kumarasinghe
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
"""

from __future__ import annotations

from typing import Any, Dict
from fastapi import APIRouter
from store.client import get_redis, is_using_fallback
from api.routes.exception import handle_exceptions

router = APIRouter(tags=["Health"])


@router.get("/health")
@handle_exceptions
async def health() -> Dict[str, Any]:
    await get_redis()
    return {
        "status": "ok",
        "store": "fallback" if is_using_fallback() else "redis",
    }
