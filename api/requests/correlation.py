"""
Correlation request models.

Copyright (c) 2026 Stefan Kumarasinghe.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. See http://www.apache.org/licenses/LICENSE-2.0 for details.
"""

from __future__ import annotations

from pydantic import Field

from ._time_range import TimeRangeRequest


class CorrelateRequest(TimeRangeRequest):
    window_seconds: float = Field(default=60.0, ge=10.0, le=600.0)
