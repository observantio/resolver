"""
Change point subpackage for the Resolver engine.

This module re-exports the :class:`ChangePoint` dataclass and the
:func:`detect` function from :mod:`engine.changepoint.cusum`, giving consumers
a clean import path of ``engine.changepoint`` for change point analysis
utilities.  The implementation itself lives in ``cusum.py``.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
"""

from engine.changepoint.cusum import ChangePoint, detect

__all__ = ["ChangePoint", "detect"]
