"""
Routes initialization and shared data models for API endpoints.

Copyright (c) 2026 Stefan Kumarasinghe.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. See http://www.apache.org/licenses/LICENSE-2.0 for details.
"""

from __future__ import annotations

from importlib import import_module

from fastapi import APIRouter

ROUTE_MODULES = [
    "health",
    "analyze",
    "metrics",
    "logs",
    "traces",
    "correlation",
    "slo",
    "topology",
    "events",
    "forecast",
    "causal",
    "ml",
    "jobs",
]


def _build_router() -> APIRouter:
    router_instance = APIRouter()

    for module_name in ROUTE_MODULES:
        module = import_module(f"api.routes.{module_name}")
        router_instance.include_router(module.router)

    return router_instance


router = _build_router()

__all__ = ["router"]
