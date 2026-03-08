"""Shared time-range request models."""

from __future__ import annotations

from typing import List, Optional

from pydantic import BaseModel, Field, model_validator


class TimeRangeRequest(BaseModel):
    tenant_id: str
    start: int
    end: int
    step: str = "15s"
    services: List[str] = Field(default_factory=list)
    log_query: Optional[str] = None
    metric_queries: Optional[List[str]] = None

    @model_validator(mode="after")
    def validate_time_range(self) -> "TimeRangeRequest":
        if self.start >= self.end:
            raise ValueError("start must be less than end")
        return self
