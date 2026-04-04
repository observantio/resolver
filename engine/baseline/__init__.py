"""
Compute logic for calculating baseline statistics (mean, standard deviation, confidence intervals) for a given set of
time series data points, with optional seasonal adjustment based on hourly patterns, to assist in anomaly detection by
providing a reference point for identifying significant deviations in metric values.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
"""

from engine.baseline.compute import Baseline, compute, score

__all__ = ["Baseline", "compute", "score"]
