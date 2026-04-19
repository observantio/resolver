"""
Anomaly detection logic for time series metric data, utilizing a combination of statistical methods (z-score, MAD) and
machine learning (Isolation Forest), along with heuristics for classifying the type and severity of detected anomalies,
to provide actionable insights into potential issues in monitored systems.

Copyright (c) 2026 Stefan Kumarasinghe.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. See http://www.apache.org/licenses/LICENSE-2.0 for details.
"""

from engine.anomaly.detection import detect
from engine.anomaly.series import iter_series

__all__ = ["detect", "iter_series"]
