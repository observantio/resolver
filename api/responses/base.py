"""
Base response models and serialization helpers.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
"""

from __future__ import annotations

from typing import Any, TypeAlias, cast

import numpy as np
from pydantic import BaseModel

SerializableValue: TypeAlias = object


def _coerce(obj: SerializableValue) -> SerializableValue:
    result: SerializableValue = obj
    if isinstance(obj, dict):
        result = {k: _coerce(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        result = [_coerce(v) for v in obj]
    elif isinstance(obj, np.integer):
        result = int(obj)
    elif isinstance(obj, np.floating):
        result = float(obj)
    elif isinstance(obj, np.ndarray):
        result = obj.tolist()
    return result


class NpModel(BaseModel):
    def model_dump(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        dumped = super().model_dump(*args, **kwargs)
        return cast(dict[str, Any], _coerce(dumped))
