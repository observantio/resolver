"""
Logs analysis logic for burst detection and pattern recognition, including frequency-based burst detection and
normalized log pattern extraction with severity categorization.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
"""

from engine.logs.frequency import detect_bursts
from engine.logs.patterns import analyze

__all__ = ["analyze", "detect_bursts"]
