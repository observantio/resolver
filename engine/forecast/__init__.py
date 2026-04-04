"""
Forecasting logic for trajectory and degradation analysis, including linear trend forecasting with confidence scoring
and degradation signal analysis based on rate and acceleration of change, to predict future behavior of metrics and
identify potential issues.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
"""

from engine.forecast.trajectory import TrajectoryForecast, forecast
from engine.forecast.degradation import DegradationSignal, analyze as analyze_degradation

__all__ = ["TrajectoryForecast", "forecast", "DegradationSignal", "analyze_degradation"]
