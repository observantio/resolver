"""
Log response models.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
"""

from __future__ import annotations

from engine.enums import Severity

from .base import NpModel


class LogBurst(NpModel):
    window_start: float
    window_end: float
    rate_per_second: float
    baseline_rate: float
    ratio: float
    severity: Severity


class LogPattern(NpModel):
    pattern: str
    count: int
    first_seen: float
    last_seen: float
    rate_per_minute: float
    entropy: float
    severity: Severity
    sample: str
