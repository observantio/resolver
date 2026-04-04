"""
RCA and causal response models.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
"""

from __future__ import annotations

from typing import Dict, List, Optional

from pydantic import ConfigDict, Field

from custom_types.json import JSONDict

from engine.enums import Severity, Signal

from .base import NpModel


class ApiRootCause(NpModel):
    model_config = ConfigDict(title="ApiRootCause")

    hypothesis: str
    confidence: float = Field(ge=0.0, le=1.0)
    evidence: List[str]
    contributing_signals: List[Signal]
    recommended_action: str
    severity: Severity
    corroboration_summary: Optional[str] = None
    suppression_diagnostics: JSONDict = Field(default_factory=dict)
    selection_score_components: Dict[str, float] = Field(default_factory=dict)


# Backward-compatible alias for existing imports.
RootCause = ApiRootCause
