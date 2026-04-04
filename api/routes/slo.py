"""
SLO routes for detecting metric anomalies and changepoints based on user-defined sensitivity and thresholds.

Copyright (c) 2026 Stefan Kumarasinghe Licensed under the Apache License, Version 2.0 (the "License"); you may not use
this file except in compliance with the License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
"""

from __future__ import annotations

import logging
from fastapi import APIRouter, Depends
from api.routes.common import get_provider, safe_call
from api.routes.exception import handle_exceptions
from services.security_service import enforce_request_tenant, require_permission_dependency
from engine import anomaly
from engine.slo import evaluate as slo_evaluate, remaining_minutes
from api.requests import SloRequest
from config import settings
from custom_types.json import JSONDict

router = APIRouter(tags=["SLO"])
log = logging.getLogger(__name__)


@router.post(
    "/slo/burn",
    summary="SLO error budget burn rate",
    dependencies=[Depends(require_permission_dependency("read:rca"))],
)
@handle_exceptions
async def slo_burn(req: SloRequest) -> JSONDict:
    req = enforce_request_tenant(req)
    error_q = req.error_query or settings.slo_error_query_template.format(service=req.service)
    total_q = req.total_query or settings.slo_total_query_template.format(service=req.service)
    provider = get_provider(req.tenant_id)

    err_raw = await safe_call(provider.query_metrics(query=error_q, start=req.start, end=req.end, step=req.step))
    tot_raw = await safe_call(provider.query_metrics(query=total_q, start=req.start, end=req.end, step=req.step))

    err_series = list(anomaly.iter_series(err_raw, query_hint=error_q))
    tot_series = list(anomaly.iter_series(tot_raw, query_hint=total_q))

    alerts = []
    budget = None
    if len(err_series) != len(tot_series):
        log.warning(
            "SLO series mismatch for tenant=%s service=%s errors=%d totals=%d",
            req.tenant_id,
            req.service,
            len(err_series),
            len(tot_series),
        )

    pair_count = min(len(err_series), len(tot_series))
    for idx in range(pair_count):
        _, err_ts, err_vals = err_series[idx]
        _, _tot_ts, tot_vals = tot_series[idx]
        if len(err_vals) != len(tot_vals):
            n = min(len(err_vals), len(tot_vals))
            log.warning(
                "SLO sample length mismatch for tenant=%s service=%s pair=%d errors=%d totals=%d",
                req.tenant_id,
                req.service,
                idx,
                len(err_vals),
                len(tot_vals),
            )
            err_vals = err_vals[:n]
            tot_vals = tot_vals[:n]
            err_ts = err_ts[:n]
        alerts.extend(slo_evaluate(req.service, err_vals, tot_vals, err_ts, req.target_availability))
        budget = remaining_minutes(req.service, err_vals, tot_vals, req.target_availability)

    return {
        "burn_alerts": [a.__dict__ for a in alerts],
        "budget_status": budget.__dict__ if budget else None,
    }
