"""
Entry point for the Resolver Analysis Engine API server.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import sys
import time
from contextlib import asynccontextmanager
from typing import AsyncIterator, Dict, Optional

import httpx
import uvicorn
from fastapi import FastAPI
from fastapi.responses import JSONResponse

from api.routes import router
from api.routes.common import close_providers
from services.security_service import InternalAuthMiddleware
from config import Settings, settings
from database import init_database, init_db, dispose_database
from datasources.exceptions import BackendStartupTimeout
from services.rca_job_service import rca_job_service

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger(__name__)

_backend_ready = False
_backend_status: Dict[str, str] = {}


async def wait_for(
    name: str,
    url: str,
    timeout: float,
    headers: Optional[Dict[str, str]] = None,
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
            except (httpx.RequestError, asyncio.TimeoutError) as exc:
                log.debug("%s not reachable (attempt %d): %s", name, attempt, exc)
            await asyncio.sleep(2)
    raise BackendStartupTimeout(f"{name} did not become ready within {timeout}s")


async def _wait_for_all_bg(data_settings: Settings, tenant_id: str) -> None:
    global _backend_ready, _backend_status
    scope = {"X-Scope-OrgID": tenant_id}
    checks: list[tuple[str, str, dict[str, str], tuple[int, ...]]] = []

    from config import (
        LOGS_BACKEND_LOKI,
        METRICS_BACKEND_MIMIR,
        METRICS_BACKEND_VICTORIAMETRICS,
        TRACES_BACKEND_TEMPO,
    )

    # Logs
    if data_settings.logs_backend == LOGS_BACKEND_LOKI:
        checks.append((
            LOGS_BACKEND_LOKI,
            f"{data_settings.loki_url}/loki/api/v1/labels",
            scope,
            (200, 404),
        ))

    # Metrics
    if data_settings.metrics_backend == METRICS_BACKEND_MIMIR:
        checks.append((
            METRICS_BACKEND_MIMIR,
            f"{data_settings.mimir_url}/prometheus/api/v1/query?query=vector%281%29",
            scope,
            (200,),
        ))
    elif data_settings.metrics_backend == METRICS_BACKEND_VICTORIAMETRICS:
        checks.append((
            METRICS_BACKEND_VICTORIAMETRICS,
            f"{data_settings.victoriametrics_url}/api/v1/label/__name__/values",
            scope,
            (200,),
        ))

    # Traces
    if data_settings.traces_backend == TRACES_BACKEND_TEMPO:
        checks.append((
            TRACES_BACKEND_TEMPO,
            f"{data_settings.tempo_url}/api/echo",
            scope,
            (200,),
        ))

    log.info("Backend readiness check starting (timeout=%ds) ...", data_settings.startup_timeout)

    for name, url, hdrs, ok in checks:
        _backend_status[name] = "waiting"

    results = await asyncio.gather(
                *[wait_for(name, url, data_settings.startup_timeout, headers=hdrs, accept_status=ok)
          for name, url, hdrs, ok in checks],
        return_exceptions=True,
    )

    all_ok = True
    for (name, *_), result in zip(checks, results):
        if isinstance(result, Exception):
            log.error("%s failed readiness: %s", name, result)
            _backend_status[name] = f"failed: {result}"
            all_ok = False
        else:
            _backend_status[name] = "ready"

    if all_ok:
        _backend_ready = True
        log.info("All backends ready — engine fully operational")
    else:
        log.warning("Some backends failed readiness — partial functionality available")
        _backend_ready = False


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
    lifespan=lifespan,
)

app.add_middleware(InternalAuthMiddleware)
app.include_router(router, prefix="/api/v1")


@app.get("/api/v1/ready", tags=["health"], summary="Backend readiness probe")
async def ready() -> JSONResponse:
    code = 200 if _backend_ready else 503
    return JSONResponse(
        status_code=code,
        content={"ready": _backend_ready, "backends": _backend_status},
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
