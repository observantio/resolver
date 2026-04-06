"""
Shared deployment event model.
"""

from __future__ import annotations

from pydantic import BaseModel, Field


class DeploymentEvent(BaseModel):
    service: str
    timestamp: float
    version: str
    author: str = ""
    environment: str = "production"
    source: str = "unknown"
    metadata: dict[str, str] = Field(default_factory=dict)
