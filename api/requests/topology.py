"""
Topology request models.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
"""

from __future__ import annotations

from pydantic import BaseModel, Field


class TopologyRequest(BaseModel):
    tenant_id: str
    start: int
    end: int
    root_service: str
    max_depth: int = Field(default=6, ge=1, le=10)
