"""Shared deployment event model."""

from __future__ import annotations

from typing import Dict

from pydantic import BaseModel, Field


class DeploymentEvent(BaseModel):
    service: str
    timestamp: float
    version: str
    author: str = ""
    environment: str = "production"
    source: str = "unknown"
    metadata: Dict[str, str] = Field(default_factory=dict)
