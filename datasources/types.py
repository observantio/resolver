"""
Types related to data sources and queries.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TypeAlias

from custom_types.json import JSONDict

QueryParamValue: TypeAlias = str | int | float | bool
QueryParams: TypeAlias = Mapping[str, QueryParamValue]
TraceFilters: TypeAlias = dict[str, QueryParamValue]

__all__ = ["JSONDict", "QueryParamValue", "QueryParams", "TraceFilters"]
