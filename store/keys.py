"""
Key-value store access layer with Redis and in-memory fallback.

Copyright (c) 2026 Stefan Kumarasinghe.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. See http://www.apache.org/licenses/LICENSE-2.0 for details.
"""

from __future__ import annotations

import hashlib


def _slug(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()[:32]


def baseline(tenant_id: str, metric_name: str) -> str:
    return f"bc:{tenant_id}:baseline:{_slug(metric_name)}"


def weights(tenant_id: str) -> str:
    return f"bc:{tenant_id}:weights"


def granger(tenant_id: str, service: str) -> str:
    return f"bc:{tenant_id}:granger:{_slug(service)}"


def events(tenant_id: str) -> str:
    return f"bc:{tenant_id}:events"
