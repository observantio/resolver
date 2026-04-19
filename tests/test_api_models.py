"""
Test API models for request validation and data integrity, ensuring that incoming requests conform to expected schemas
and constraints.

Copyright (c) 2026 Stefan Kumarasinghe.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. See http://www.apache.org/licenses/LICENSE-2.0 for details.
"""

import pytest
from pydantic import ValidationError

from api.requests import AnalyzeRequest, CorrelateRequest, DeploymentEventRequest, SloRequest


def test_deployment_request_requires_tenant():
    req = DeploymentEventRequest(tenant_id="t1", service="s", timestamp=1.0, version="v1")
    assert req.tenant_id == "t1"
    with pytest.raises(ValidationError):
        DeploymentEventRequest.model_validate({"service": "s", "timestamp": 1.0, "version": "v1"})


def test_time_range_validations():
    with pytest.raises(ValidationError):
        AnalyzeRequest(tenant_id="t1", start=10, end=10)
    with pytest.raises(ValidationError):
        CorrelateRequest(tenant_id="t1", start=11, end=10)
    with pytest.raises(ValidationError):
        SloRequest(tenant_id="t1", service="svc", start=5, end=5)
