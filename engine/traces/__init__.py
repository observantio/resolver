"""
Package for trace analysis, including latency analysis and error propagation detection.

Copyright (c) 2026 Stefan Kumarasinghe.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. See http://www.apache.org/licenses/LICENSE-2.0 for details.
"""

from engine.traces.errors import detect_propagation
from engine.traces.latency import analyze

__all__ = ["analyze", "detect_propagation"]
