"""
Time range request model for log and metric queries.

Copyright (c) 2026 Stefan Kumarasinghe.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. See http://www.apache.org/licenses/LICENSE-2.0 for details.
"""

from __future__ import annotations

from pydantic import BaseModel, Field, model_validator


class TimeRangeRequest(BaseModel):
    tenant_id: str
    start: int
    end: int
    step: str = "15s"
    services: list[str] = Field(default_factory=list)
    log_query: str | None = None
    metric_queries: list[str] | None = None

    @model_validator(mode="after")
    def validate_time_range(self) -> TimeRangeRequest:
        if self.start >= self.end:
            raise ValueError("start must be less than end")
        return self
