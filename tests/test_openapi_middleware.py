"""
OpenAPI middleware tests.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
"""

from __future__ import annotations

from fastapi import FastAPI

from middleware import openapi as openapi_middleware


def test_apply_inferred_responses_handles_non_dict_responses() -> None:
    operation: dict[str, object] = {"responses": []}
    openapi_middleware._apply_inferred_responses("/api/v1/jobs/{job_id}", "POST", operation)  # type: ignore[arg-type]
    assert operation["responses"] == []


def test_apply_inferred_responses_for_secured_and_ready_paths() -> None:
    secured: dict[str, object] = {"requestBody": {"content": {}}}
    openapi_middleware._apply_inferred_responses("/api/v1/jobs/{job_id}", "POST", secured)  # type: ignore[arg-type]
    secured_responses = secured["responses"]
    assert isinstance(secured_responses, dict)
    assert secured_responses["401"]["description"] == "Unauthorized"
    assert secured_responses["403"]["description"] == "Forbidden"
    assert secured_responses["400"]["description"] == "Bad Request"
    assert secured_responses["404"]["description"] == "Not Found"

    ready: dict[str, object] = {}
    openapi_middleware._apply_inferred_responses("/api/v1/ready", "GET", ready)  # type: ignore[arg-type]
    ready_responses = ready["responses"]
    assert isinstance(ready_responses, dict)
    assert "401" not in ready_responses
    assert "403" not in ready_responses


def test_install_custom_openapi_cache_and_schema_walk(monkeypatch) -> None:
    app = FastAPI()
    app.openapi_schema = {"cached": True}
    openapi_middleware.install_custom_openapi(app)
    assert app.openapi() == {"cached": True}

    app2 = FastAPI()
    openapi_middleware.install_custom_openapi(app2)
    fake_schema = {
        "paths": {
            "/api/v1/jobs/{job_id}": {
                "post": {"requestBody": {"content": {}}},
                "trace": "skip",
            },
            "/api/v1/ready": {"get": {}},
            "/api/v1/skip": "skip",
        }
    }
    monkeypatch.setattr(openapi_middleware, "get_openapi", lambda **kwargs: fake_schema)
    generated = app2.openapi()
    responses = generated["paths"]["/api/v1/jobs/{job_id}"]["post"]["responses"]
    assert responses["401"]["description"] == "Unauthorized"
    assert responses["403"]["description"] == "Forbidden"
    assert responses["400"]["description"] == "Bad Request"
    assert responses["404"]["description"] == "Not Found"

    app3 = FastAPI()
    openapi_middleware.install_custom_openapi(app3)
    monkeypatch.setattr(openapi_middleware, "get_openapi", lambda **kwargs: {"paths": []})
    assert app3.openapi()["paths"] == []
