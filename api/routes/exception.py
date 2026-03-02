"""
Centralized exception handling decorator for API route functions.

The :func:`handle_exceptions` decorator wraps an endpoint handler, catching any
uncaught exceptions and converting them into :class:`fastapi.HTTPException`
responses.  HTTPExceptions raised by the handler are propagated untouched, thus
preserving status codes and detail messages defined locally.  All other
exceptions are turned into a ``500`` error with the exception message as the
response detail.


Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
"""

from __future__ import annotations

import inspect
from functools import wraps
from typing import Any, Callable, TypeVar, cast
from fastapi import HTTPException

F = TypeVar("F", bound=Callable[..., Any])


def handle_exceptions(func: F) -> F:

    if inspect.iscoroutinefunction(func):
        @wraps(func)
        async def async_wrapper(*args: Any, **kwargs: Any) -> Any: 
            try:
                return await func(*args, **kwargs)
            except HTTPException:
                raise
            except Exception as exc:  
                raise HTTPException(status_code=500, detail=str(exc)) from exc

        return cast(F, async_wrapper)

    @wraps(func)
    def sync_wrapper(*args: Any, **kwargs: Any) -> Any:  
        try:
            return func(*args, **kwargs)
        except HTTPException:
            raise
        except Exception as exc: 
            raise HTTPException(status_code=500, detail=str(exc)) from exc

    return cast(F, sync_wrapper)
