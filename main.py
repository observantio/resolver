"""
Entry point for the Resolver Analysis Engine API server.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import sys
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import httpx
import uvicorn
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.routing import APIRoute
from pydantic import BaseModel, Field

from api.routes import router
from api.routes.common import close_providers
from config import LOGS_BACKEND_LOKI, METRICS_BACKEND_MIMIR, TRACES_BACKEND_TEMPO, Settings, settings
from database import dispose_database, init_database, init_db
from datasources.exceptions import BackendStartupTimeout
from middleware.openapi import install_custom_openapi
from services.rca_job_service import rca_job_service
from services.security_service import InternalAuthMiddleware

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger(__name__)

_BACKEND_READY = False
_BACKEND_STATUS: dict[str, str] = {}
OPENAPI_TAGS = [
    {"name": "Health", "description": "Service and backend readiness endpoints."},
    {"name": "RCA", "description": "Root cause analysis workflows and templates."},
    {"name": "Metrics", "description": "Metric anomaly and changepoint analysis."},
    {"name": "Logs", "description": "Log pattern and burst analysis."},
    {"name": "Traces", "description": "Trace latency and error propagation analysis."},
    {"name": "Correlation", "description": "Cross-signal temporal correlation endpoints."},
    {"name": "SLO", "description": "Service-level objective error budget analysis."},
    {"name": "Topology", "description": "Service dependency and blast radius analysis."},
    {"name": "Events", "description": "Deployment event ingestion and lifecycle endpoints."},
    {"name": "Forecast", "description": "Degradation trajectory and time-to-failure forecasting."},
    {"name": "Causal", "description": "Causality analysis across metric and signal features."},
    {"name": "ML", "description": "Adaptive signal-weight model controls and feedback."},
    {"name": "RCA Jobs", "description": "Asynchronous RCA job creation, polling, and reports."},
]


def _openapi_servers() -> list[dict[str, str]]:
    scheme = "https" if settings.ssl_enabled else "http"
    explicit = f"{scheme}://{settings.host}:{settings.port}"
    servers = [{"url": "/"}]
    if settings.host and settings.port:
        servers.append({"url": explicit})
    return servers


def _generate_operation_id(route: APIRoute) -> str:
    return route.name


class ResolverReadyResponse(BaseModel):
    ready: bool = Field(description="Whether resolver dependencies are currently ready.")
    backends: dict[str, str] = Field(
        default_factory=dict,
        description="Per-backend readiness details keyed by backend name.",
    )


async def wait_for(
    name: str,
    url: str,
    timeout: float,
    headers: dict[str, str] | None = None,
    accept_status: tuple[int, ...] = (200, 204, 404),
) -> None:
    deadline = time.monotonic() + timeout
    attempt = 0
    async with httpx.AsyncClient() as client:
        while time.monotonic() < deadline:
            attempt += 1
            try:
                resp = await client.get(url, headers=headers or {}, timeout=3.0)
                if resp.status_code in accept_status:
                    log.info("%s ready (attempt %d, status %d)", name, attempt, resp.status_code)
                    return
                log.debug("%s probe returned %d (attempt %d)", name, resp.status_code, attempt)
            except (TimeoutError, httpx.RequestError) as exc:
                log.debug("%s not reachable (attempt %d): %s", name, attempt, exc)
            await asyncio.sleep(2)
    raise BackendStartupTimeout(f"{name} did not become ready within {timeout}s")


async def _wait_for_all_bg(data_settings: Settings, tenant_id: str) -> None:
    scope = {"X-Scope-OrgID": tenant_id}
    checks: list[tuple[str, str, dict[str, str], tuple[int, ...]]] = []

    # Logs
    if data_settings.logs_backend == LOGS_BACKEND_LOKI:
        checks.append(
            (
                LOGS_BACKEND_LOKI,
                f"{data_settings.loki_url}/loki/api/v1/labels",
                scope,
                (200, 404),
            )
        )

    # Metrics
    if data_settings.metrics_backend == METRICS_BACKEND_MIMIR:
        checks.append(
            (
                METRICS_BACKEND_MIMIR,
                f"{data_settings.mimir_url}/prometheus/api/v1/query?query=vector%281%29",
                scope,
                (200,),
            )
        )

    # Traces
    if data_settings.traces_backend == TRACES_BACKEND_TEMPO:
        checks.append(
            (
                TRACES_BACKEND_TEMPO,
                f"{data_settings.tempo_url}/api/echo",
                scope,
                (200,),
            )
        )

    log.info("Backend readiness check starting (timeout=%ds) ...", data_settings.startup_timeout)

    for name, url, hdrs, ok in checks:
        _BACKEND_STATUS[name] = "waiting"

    results = await asyncio.gather(
        *[
            wait_for(name, url, data_settings.startup_timeout, headers=hdrs, accept_status=ok)
            for name, url, hdrs, ok in checks
        ],
        return_exceptions=True,
    )

    all_ok = True
    for (name, *_), result in zip(checks, results):
        if isinstance(result, Exception):
            log.error("%s failed readiness: %s", name, result)
            _BACKEND_STATUS[name] = f"failed: {result}"
            all_ok = False
        else:
            _BACKEND_STATUS[name] = "ready"

    if all_ok:
        globals()["_BACKEND_READY"] = True
        log.info("All backends ready — engine fully operational")
    else:
        log.warning("Some backends failed readiness — partial functionality available")
        globals()["_BACKEND_READY"] = False


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
    if settings.database_url:
        init_database(settings.database_url)
        init_db()
        await rca_job_service.startup_recovery()

    tenant_id = settings.default_tenant_id
    readiness_task = asyncio.create_task(_wait_for_all_bg(settings, tenant_id))
    cleanup_task = asyncio.create_task(_cleanup_loop())
    try:
        yield
    finally:
        readiness_task.cancel()
        cleanup_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await readiness_task
        with contextlib.suppress(asyncio.CancelledError):
            await cleanup_task
        await close_providers()
        dispose_database()


async def _cleanup_loop() -> None:
    while True:
        await asyncio.sleep(300)
        if settings.database_url:
            await rca_job_service.cleanup_retention()


app = FastAPI(
    title="Resolver Analysis Engine",
    description="AI-powered root cause analysis and anomaly detection over logs, metrics, and traces.",
    version="1.0.0",
    generate_unique_id_function=_generate_operation_id,
    servers=_openapi_servers(),
    contact={
        "name": "Resolver Maintainers",
        "url": "https://github.com/stefankhacks/watchdog",
    },
    license_info={
        "name": "Apache 2.0",
        "identifier": "Apache-2.0",
    },
    openapi_tags=OPENAPI_TAGS,
    lifespan=lifespan,
)

app.add_middleware(InternalAuthMiddleware)
app.include_router(router, prefix="/api/v1")
install_custom_openapi(app)


@app.get(
    "/api/v1/ready",
    tags=["Health"],
    summary="Backend readiness probe",
    description="Returns readiness state for configured backend dependencies.",
    response_description="Resolver readiness state and per-backend status details.",
    responses={
        200: {"model": ResolverReadyResponse},
        503: {"model": ResolverReadyResponse, "description": "Service Unavailable"},
    },
)
async def ready() -> JSONResponse:
    code = 200 if _BACKEND_READY else 503
    return JSONResponse(
        status_code=code,
        content={"ready": _BACKEND_READY, "backends": _BACKEND_STATUS},
    )


if __name__ == "__main__":
    if settings.ssl_enabled:
        uvicorn.run(
            "main:app",
            host=settings.host,
            port=settings.port,
            log_level="info",
            access_log=True,
            ssl_certfile=settings.ssl_certfile,
            ssl_keyfile=settings.ssl_keyfile,
        )
    else:
        uvicorn.run(
            "main:app",
            host=settings.host,
            port=settings.port,
            log_level="info",
            access_log=True,
        )
