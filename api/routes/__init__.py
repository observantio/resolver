"""
Routes initialization and shared data models for API endpoints.

Copyright (c) 2026 Stefan Kumarasinghe.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. See http://www.apache.org/licenses/LICENSE-2.0 for details.
"""

from __future__ import annotations

from fastapi import APIRouter

_router: APIRouter | None = None


def _build_router() -> APIRouter:
	router = APIRouter()

	from api.routes.analyze import router as analyze_router
	from api.routes.causal import router as causal_router
	from api.routes.correlation import router as correlation_router
	from api.routes.events import router as events_router
	from api.routes.forecast import router as forecast_router
	from api.routes.health import router as health_router
	from api.routes.jobs import router as jobs_router
	from api.routes.logs import router as logs_router
	from api.routes.metrics import router as metrics_router
	from api.routes.ml import router as ml_router
	from api.routes.slo import router as slo_router
	from api.routes.topology import router as topology_router
	from api.routes.traces import router as traces_router

	router.include_router(health_router)
	router.include_router(analyze_router)
	router.include_router(metrics_router)
	router.include_router(logs_router)
	router.include_router(traces_router)
	router.include_router(correlation_router)
	router.include_router(slo_router)
	router.include_router(topology_router)
	router.include_router(events_router)
	router.include_router(forecast_router)
	router.include_router(causal_router)
	router.include_router(ml_router)
	router.include_router(jobs_router)

	return router


def __getattr__(name: str) -> APIRouter:
	if name != "router":
		raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

	global _router
	if _router is None:
		_router = _build_router()
		globals()["router"] = _router
	return _router

__all__ = ["router"]
