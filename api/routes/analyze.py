"""
Analyze route for root cause analysis (RCA) across multiple signals. This gives a comprehensive analysis report
including metric anomalies, log bursts, service latency issues, error propagation, and more. You may filter or specify
time ranges and other parameters in the AnalyzeRequest to focus the analysis.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
"""

from __future__ import annotations

from fastapi import APIRouter, Depends
from api.routes.exception import handle_exceptions
from api.requests import AnalyzeRequest
from api.responses import AnalysisReport, AnalyzeConfigTemplateResponse
from services.analyze_service import run_analysis
from services.analysis_config_service import analysis_config_service
from services.security_service import require_permission_dependency

router = APIRouter(tags=["RCA"])


@router.post(
    "/analyze",
    response_model=AnalysisReport,
    summary="Full cross-signal RCA",
    dependencies=[Depends(require_permission_dependency("create:rca"))],
)
@handle_exceptions
async def analyze(req: AnalyzeRequest) -> AnalysisReport:
    return await run_analysis(req)


@router.get(
    "/analyze/config-template",
    response_model=AnalyzeConfigTemplateResponse,
    summary="Default RCA YAML config template",
    dependencies=[Depends(require_permission_dependency("create:rca"))],
)
async def analyze_config_template() -> AnalyzeConfigTemplateResponse:
    return AnalyzeConfigTemplateResponse.model_validate(analysis_config_service.template_response())
