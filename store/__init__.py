"""
Initialization of the store package, exposing client functions and submodules.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
"""

from store.client import redis_get, redis_set, redis_delete, is_using_fallback
from store import baseline, weights, granger, events

__all__ = [
    "redis_get",
    "redis_set",
    "redis_delete",
    "is_using_fallback",
    "baseline",
    "weights",
    "granger",
    "events",
]
