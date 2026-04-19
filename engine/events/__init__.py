"""
Event handling and registry for deployment events, providing a structured way to record and query deployment-related
information such as service name, timestamp, version, author, environment, source, and additional metadata, to
facilitate correlation with observed anomalies and support root cause analysis.

Copyright (c) 2026 Stefan Kumarasinghe.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. See http://www.apache.org/licenses/LICENSE-2.0 for details.
"""

from engine.events.registry import DeploymentEvent, EventRegistry

__all__ = ["DeploymentEvent", "EventRegistry"]
