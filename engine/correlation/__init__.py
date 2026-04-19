"""
Correlation logic for identifying related anomalies across different signals (metrics, logs, traces) based on temporal
proximity and other heuristics, to assist in root cause analysis and incident investigation.

Copyright (c) 2026 Stefan Kumarasinghe.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. See http://www.apache.org/licenses/LICENSE-2.0 for details.
"""

from engine.correlation.signals import LogMetricLink, link_logs_to_metrics
from engine.correlation.temporal import CorrelatedEvent, correlate

__all__ = ["CorrelatedEvent", "LogMetricLink", "correlate", "link_logs_to_metrics"]
