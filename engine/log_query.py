"""
Shared helpers for building log queries.

Copyright (c) 2026 Stefan Kumarasinghe.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. See http://www.apache.org/licenses/LICENSE-2.0 for details.
"""

from __future__ import annotations

import re


def build_log_query(services: list[str] | None, requested_log_query: str | None) -> str:
    requested = (requested_log_query or "").strip()
    if requested:
        return re.sub(r'=~"\.\*"', '=~".+"', requested)
    if services:
        escaped = [re.escape(service) for service in services if service]
        if escaped:
            return '{service_name=~"' + "|".join(escaped) + '"}'
    return '{service_name=~".+"}'
