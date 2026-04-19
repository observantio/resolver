"""
Centralized exception handling decorator for API route functions.

The :func:`handle_exceptions` decorator wraps an endpoint handler, catching any
uncaught exceptions and converting them into :class:`fastapi.HTTPException`
responses.  HTTPExceptions raised by the handler are propagated untouched, thus
preserving status codes and detail messages defined locally.  All other
exceptions are turned into a ``500`` error with the exception message as the
response detail.


Copyright (c) 2026 Stefan Kumarasinghe.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
See http://www.apache.org/licenses/LICENSE-2.0 for details.
"""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable
from functools import wraps
from typing import TypeVar, cast

from fastapi import HTTPException

F = TypeVar("F", bound=Callable[..., object])


def handle_exceptions(func: F) -> F:

    if inspect.iscoroutinefunction(func):
        async_func = cast(Callable[..., Awaitable[object]], func)

        @wraps(func)
        async def async_wrapper(*args: object, **kwargs: object) -> object:
            try:
                return await async_func(*args, **kwargs)
            except HTTPException:
                raise
            except Exception as exc:
                raise HTTPException(status_code=500, detail=str(exc)) from exc

        return cast(F, async_wrapper)

    sync_func = cast(Callable[..., object], func)

    @wraps(func)
    def sync_wrapper(*args: object, **kwargs: object) -> object:
        try:
            return sync_func(*args, **kwargs)
        except HTTPException:
            raise
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc

    return cast(F, sync_wrapper)
