"""
Deployment event request models.

Copyright (c) 2026 Stefan Kumarasinghe.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. See http://www.apache.org/licenses/LICENSE-2.0 for details.
"""

from __future__ import annotations

from engine.events.models import DeploymentEvent


class DeploymentEventRequest(DeploymentEvent):
    tenant_id: str
