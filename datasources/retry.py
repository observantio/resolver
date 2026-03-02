"""
Retry decorator for connector methods.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
"""

from __future__ import annotations

import asyncio
import inspect
import time
from functools import wraps
from typing import Any, Callable, Type, TypeVar, Tuple, cast

F = TypeVar("F", bound=Callable[..., Any])

def retry(
    *,
    attempts: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0,
    exceptions: Tuple[Type[Exception], ...] = (Exception,),
) -> Callable[[F], F]:
    def decorator(func: F) -> F:
        is_async = inspect.iscoroutinefunction(func)

        if is_async:
            @wraps(func)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:  
                _attempt = 0
                _delay = delay
                while True:
                    try:
                        return await func(*args, **kwargs)
                    except exceptions as exc: 
                        _attempt += 1
                        if _attempt >= attempts:
                            raise
                        await asyncio.sleep(_delay)
                        _delay *= backoff

            return cast(F, async_wrapper)

        else:
            @wraps(func)
            def sync_wrapper(*args: Any, **kwargs: Any) -> Any:  
                _attempt = 0
                _delay = delay
                while True:
                    try:
                        return func(*args, **kwargs)
                    except exceptions as exc: 
                        _attempt += 1
                        if _attempt >= attempts:
                            raise
                        time.sleep(_delay)
                        _delay *= backoff

            return cast(F, sync_wrapper)

    return decorator
