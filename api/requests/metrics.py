"""
Metrics request models.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
"""

from __future__ import annotations

from pydantic import BaseModel, Field


class MetricRequest(BaseModel):
    tenant_id: str
    query: str
    start: int
    end: int
    step: str = "15s"
    sensitivity: float | None = Field(default=3.0, ge=1.0, le=6.0)


class ChangepointRequest(BaseModel):
    tenant_id: str
    query: str
    start: int
    end: int
    step: str = "15s"
    threshold_sigma: float = Field(default=4.0, ge=1.0, le=10.0)
