"""
Shared deployment event model.

Copyright (c) 2026 Stefan Kumarasinghe.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. See http://www.apache.org/licenses/LICENSE-2.0 for details.
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
