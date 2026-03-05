"""
Correlation request models.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
"""

from __future__ import annotations

from typing import List, Optional

from pydantic import BaseModel, Field, model_validator


class CorrelateRequest(BaseModel):
    tenant_id: str
    start: int
    end: int
    step: str = "15s"
    services: List[str] = Field(default_factory=list)
    log_query: Optional[str] = None
    metric_queries: Optional[List[str]] = None
    window_seconds: float = Field(default=60.0, ge=10.0, le=600.0)

    @model_validator(mode="after")
    def validate_time_range(self) -> "CorrelateRequest":
        if self.start >= self.end:
            raise ValueError("start must be less than end")
        return self
