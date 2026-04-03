"""
Base response models and serialization helpers.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
"""

from __future__ import annotations

from typing import Any, TypeAlias, cast

import numpy as np
from pydantic import BaseModel

SerializableValue: TypeAlias = object


def _coerce(obj: SerializableValue) -> SerializableValue:
    if isinstance(obj, dict):
        return {k: _coerce(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_coerce(v) for v in obj]
    if isinstance(obj, np.integer):
        return int(obj)
    if isinstance(obj, np.floating):
        return float(obj)
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    return obj


class NpModel(BaseModel):
    def model_dump(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        dumped = super().model_dump(*args, **kwargs)
        return cast(dict[str, Any], _coerce(dumped))
