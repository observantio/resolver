"""
SLO request models.

Copyright (c) 2026 Stefan Kumarasinghe.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. See http://www.apache.org/licenses/LICENSE-2.0 for details.
"""

from __future__ import annotations

from pydantic import BaseModel, Field, model_validator


class SloRequest(BaseModel):
    tenant_id: str
    service: str
    start: int
    end: int
    step: str = "15s"
    target_availability: float = Field(default=0.999, ge=0.0, le=1.0)
    error_query: str | None = None
    total_query: str | None = None

    @model_validator(mode="after")
    def validate_time_range(self) -> SloRequest:
        if self.start >= self.end:
            raise ValueError("start must be less than end")
        return self
