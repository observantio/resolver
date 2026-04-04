"""
Retry decorator for connector methods.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
"""

from __future__ import annotations

import asyncio
import inspect
import time
from functools import wraps
from typing import Awaitable, Callable, Type, Tuple, TypeVar, cast

F = TypeVar("F", bound=Callable[..., object])


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
            async_func = cast(Callable[..., Awaitable[object]], func)

            @wraps(func)
            async def async_wrapper(*args: object, **kwargs: object) -> object:
                _attempt = 0
                _delay = delay
                while True:
                    try:
                        return await async_func(*args, **kwargs)
                    except exceptions:
                        _attempt += 1
                        if _attempt >= attempts:
                            raise
                        await asyncio.sleep(_delay)
                        _delay *= backoff

            return cast(F, async_wrapper)

        sync_func = cast(Callable[..., object], func)

        @wraps(func)
        def sync_wrapper(*args: object, **kwargs: object) -> object:
            _attempt = 0
            _delay = delay
            while True:
                try:
                    return sync_func(*args, **kwargs)
                except exceptions:
                    _attempt += 1
                    if _attempt >= attempts:
                        raise
                    time.sleep(_delay)
                    _delay *= backoff

        return cast(F, sync_wrapper)

    return decorator
