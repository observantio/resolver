"""
Topology analysis routes for computing service dependency blast radius and upstream/downstream relationships from trace data.

Copyright (c) 2026 Stefan Kumarasinghe
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
"""

from __future__ import annotations

from typing import Any, Dict
from fastapi import APIRouter, Depends
from api.routes.common import get_provider, safe_call
from api.routes.exception import handle_exceptions
from services.security_service import enforce_request_tenant, require_permission_dependency
from engine.topology import DependencyGraph
from api.requests import TopologyRequest

router = APIRouter(tags=["Topology"])


@router.post(
    "/topology/blast-radius",
    summary="Service dependency blast radius from traces",
    dependencies=[Depends(require_permission_dependency("read:rca"))],
)
@handle_exceptions
async def blast_radius(req: TopologyRequest) -> Dict[str, Any]:
    req = enforce_request_tenant(req)
    raw = await safe_call(
        get_provider(req.tenant_id).query_traces(
            filters={}, start=req.start, end=req.end
        )
    )

    graph = DependencyGraph()
    graph.from_spans(raw)

    radius = graph.blast_radius(req.root_service, max_depth=req.max_depth)
    upstream = graph.find_upstream_roots(req.root_service)

    return {
        "root_service": radius.root_service,
        "affected_downstream": radius.affected_downstream,
        "upstream_roots": upstream,
        "all_services": sorted(graph.all_services()),
    }
