"""
Package for trace analysis, including latency analysis and error propagation detection.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
"""

from engine.traces.errors import detect_propagation
from engine.traces.latency import analyze

__all__ = ["analyze", "detect_propagation"]
