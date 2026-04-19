"""
OpenAPI middleware tests.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
"""

from __future__ import annotations

import tomllib

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
    assert ready_responses["503"]["description"] == "Service Unavailable"


def test_project_version_helpers_cover_success_and_fallback(monkeypatch) -> None:
    original_read_text = openapi_middleware.Path.read_text

    monkeypatch.setattr(
        openapi_middleware.Path,
        "read_text",
        lambda *args, **kwargs: "[project]\nversion = '9.9.9'\n",
    )
    assert openapi_middleware._project_version() == "9.9.9"

    monkeypatch.setattr(
        openapi_middleware.Path,
        "read_text",
        lambda *args, **kwargs: "[project]\nversion = ''\n",
    )
    assert openapi_middleware._project_version() == openapi_middleware._DEFAULT_APP_VERSION

    monkeypatch.setattr(
        openapi_middleware.Path,
        "read_text",
        lambda *args, **kwargs: (_ for _ in ()).throw(OSError("boom")),
    )
    assert openapi_middleware._project_version() == openapi_middleware._DEFAULT_APP_VERSION

    monkeypatch.setattr(
        openapi_middleware.Path,
        "read_text",
        lambda *args, **kwargs: "bad-toml",
    )
    monkeypatch.setattr(
        openapi_middleware.tomllib,
        "loads",
        lambda _text: (_ for _ in ()).throw(tomllib.TOMLDecodeError("bad", "", 0)),
    )
    assert openapi_middleware._project_version() == openapi_middleware._DEFAULT_APP_VERSION

    monkeypatch.setattr(openapi_middleware.Path, "read_text", original_read_text)


def test_install_custom_openapi_sets_info_version(monkeypatch) -> None:
    app = FastAPI(title="Resolver", version="ignored", description="desc")
    openapi_middleware.install_custom_openapi(app)

    monkeypatch.setattr(
        openapi_middleware,
        "get_openapi",
        lambda **_kwargs: {"info": {"title": "Resolver"}, "paths": {}},
    )
    monkeypatch.setattr(openapi_middleware, "_project_version", lambda: "1.2.3")

    generated = app.openapi()
    assert generated["info"]["version"] == "1.2.3"


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
            "/api/v1/events/deployment": {
                "post": {
                    "parameters": [
                        {"name": "tenant_id", "in": "query"},
                        {"name": "keep_me", "in": "query"},
                    ],
                    "responses": {"400": {"description": "Bad Request"}},
                }
            },
            "/api/v1/skip": "skip",
        },
        "components": {
            "schemas": {
                "AnalyzeJobResultResponse": {
                    "properties": {
                        "result": {
                            "anyOf": [
                                {"type": "object", "additionalProperties": {"type": "string"}},
                                {"type": "null"},
                            ]
                        }
                    }
                },
                "AnalyzeReportResponse": {
                    "properties": {
                        "result": {
                            "anyOf": [
                                {"type": "object", "additionalProperties": {"type": "string"}},
                                {"type": "null"},
                            ]
                        }
                    }
                },
                "MetricRequest": {"properties": {"step": {"type": "string"}}},
            }
        },
    }
    monkeypatch.setattr(openapi_middleware, "get_openapi", lambda **kwargs: fake_schema)
    generated = app2.openapi()
    responses = generated["paths"]["/api/v1/jobs/{job_id}"]["post"]["responses"]
    assert responses["401"]["description"] == "Unauthorized"
    assert responses["403"]["description"] == "Forbidden"
    assert responses["400"]["description"] == "Bad Request"
    assert responses["404"]["description"] == "Not Found"
    assert responses["401"]["content"]["application/json"]["schema"]["$ref"] == "#/components/schemas/ErrorResponse"
    assert generated["paths"]["/api/v1/jobs/{job_id}"]["post"]["security"] == [
        {"ServiceToken": [], "ContextBearer": []}
    ]
    ready_responses = generated["paths"]["/api/v1/ready"]["get"]["responses"]
    assert ready_responses["503"]["description"] == "Service Unavailable"
    assert generated["jsonSchemaDialect"] == "https://spec.openapis.org/oas/3.1/dialect/base"
    schemes = generated["components"]["securitySchemes"]
    assert schemes["ServiceToken"]["name"] == "x-service-token"
    assert schemes["ContextBearer"]["scheme"] == "bearer"
    deployment_params = generated["paths"]["/api/v1/events/deployment"]["post"]["parameters"]
    assert deployment_params == [{"name": "keep_me", "in": "query"}]
    assert generated["components"]["schemas"]["ErrorResponse"]["required"] == ["detail"]
    assert generated["components"]["schemas"]["AnalyzeJobResultResponse"]["properties"]["result"]["anyOf"][0] == {
        "$ref": "#/components/schemas/AnalysisReport"
    }
    assert generated["components"]["schemas"]["AnalyzeReportResponse"]["properties"]["result"]["anyOf"][0] == {
        "$ref": "#/components/schemas/AnalysisReport"
    }
    assert (
        generated["components"]["schemas"]["MetricRequest"]["properties"]["step"]["pattern"] == r"^[1-9][0-9]*[smhd]$"
    )

    app3 = FastAPI()
    openapi_middleware.install_custom_openapi(app3)
    monkeypatch.setattr(openapi_middleware, "get_openapi", lambda **kwargs: {"paths": []})
    assert app3.openapi()["paths"] == []

    app4 = FastAPI()
    openapi_middleware.install_custom_openapi(app4)
    monkeypatch.setattr(openapi_middleware, "get_openapi", lambda **kwargs: [])  # type: ignore[return-value]
    assert app4.openapi() == []


def test_openapi_doc_helpers_cover_fallbacks() -> None:
    assert openapi_middleware._status_description(999) == "HTTP 999"

    operation: dict[str, object] = {
        "operationId": "list_jobs",
        "responses": {
            "200": {},
            "201": {"description": "Created"},
            "2xx": {},
        },
    }
    openapi_middleware._ensure_operation_docs("/api/v1/jobs/{job_id}", "GET", operation)  # type: ignore[arg-type]
    assert operation["summary"] == "List Jobs"
    assert "description" not in operation
    responses = operation["responses"]
    assert isinstance(responses, dict)
    assert responses["200"]["description"] == "OK"
    assert responses["201"]["description"] == "Created"
    assert responses["2xx"]["description"] == "Success"

    no_operation_id: dict[str, object] = {"responses": {"200": {"description": "Successful Response"}}}
    openapi_middleware._ensure_operation_docs("/", "TRACE", no_operation_id)  # type: ignore[arg-type]
    assert no_operation_id["summary"] == "TRACE Root"

    prefilled: dict[str, object] = {
        "summary": "Keep Summary",
        "description": "Keep description.",
        "responses": {
            "200": "skip",
            "201": {"description": "Successful Response"},
        },
    }
    openapi_middleware._ensure_operation_docs("/api/v1/jobs", "GET", prefilled)  # type: ignore[arg-type]
    assert prefilled["summary"] == "Keep Summary"
    assert prefilled["description"] == "Keep description."
    prefilled_responses = prefilled["responses"]
    assert isinstance(prefilled_responses, dict)
    assert prefilled_responses["200"] == "skip"
    assert prefilled_responses["201"]["description"] == "Created"

    non_dict_responses: dict[str, object] = {"responses": []}
    openapi_middleware._ensure_operation_docs("/api/v1/jobs", "GET", non_dict_responses)  # type: ignore[arg-type]
    assert non_dict_responses["summary"] == "Retrieve Jobs"


def test_security_helpers_cover_guard_and_existing_security() -> None:
    schema_bad_components = {"components": []}
    openapi_middleware._ensure_security_schemes(schema_bad_components)  # type: ignore[arg-type]
    assert schema_bad_components["components"] == []

    schema_bad_security = {"components": {"securitySchemes": []}}
    openapi_middleware._ensure_security_schemes(schema_bad_security)  # type: ignore[arg-type]
    assert schema_bad_security["components"]["securitySchemes"] == []

    operation = {"security": [{"ServiceToken": [], "ContextBearer": []}]}
    openapi_middleware._apply_operation_security("/api/v1/jobs", operation)  # type: ignore[arg-type]
    assert operation["security"] == [{"ServiceToken": [], "ContextBearer": []}]

    ready_operation: dict[str, object] = {}
    openapi_middleware._apply_operation_security("/api/v1/ready", ready_operation)  # type: ignore[arg-type]
    assert "security" not in ready_operation


def test_openapi_helper_guards_cover_remaining_branches() -> None:
    schema_bad_components = {"components": []}
    openapi_middleware._ensure_error_schema(schema_bad_components)  # type: ignore[arg-type]
    assert schema_bad_components["components"] == []

    schema_bad_schemas = {"components": {"schemas": []}}
    openapi_middleware._ensure_error_schema(schema_bad_schemas)  # type: ignore[arg-type]
    assert schema_bad_schemas["components"]["schemas"] == []

    operation_non_dict: dict[str, object] = {"responses": []}
    openapi_middleware._ensure_standard_error_content(operation_non_dict)  # type: ignore[arg-type]
    assert operation_non_dict["responses"] == []

    mixed_responses: dict[str, object] = {
        "responses": {
            "400": "skip",
            "418": {"description": "Teapot"},
            "401": {"description": "Unauthorized", "content": {}},
        }
    }
    openapi_middleware._ensure_standard_error_content(mixed_responses)  # type: ignore[arg-type]
    responses = mixed_responses["responses"]
    assert isinstance(responses, dict)
    assert responses["400"] == "skip"
    assert "content" not in responses["418"]
    assert responses["401"]["content"] == {}

    generic_description: dict[str, object] = {
        "description": "Handles GET requests for `/api/v1/jobs`.",
        "responses": {"200": {}},
    }
    openapi_middleware._ensure_operation_docs("/api/v1/jobs", "GET", generic_description)  # type: ignore[arg-type]
    assert "description" not in generic_description

    non_string_description: dict[str, object] = {
        "description": {"text": "bad"},
        "responses": {"200": {}},
    }
    openapi_middleware._ensure_operation_docs("/api/v1/jobs", "GET", non_string_description)  # type: ignore[arg-type]
    assert "description" not in non_string_description

    assert (
        openapi_middleware._iter_method_operations(
            {
                "/api/v1/jobs": {
                    "summary": "skip",
                    "x-custom": {},
                    "get": "not-a-dict",
                }
            }
        )
        == []
    )

    paths_not_dict: dict[str, object] = {"/api/v1/events/deployment": []}
    openapi_middleware._remove_deployment_tenant_query_param(paths_not_dict)  # type: ignore[arg-type]
    assert paths_not_dict["/api/v1/events/deployment"] == []

    paths_missing_post: dict[str, object] = {"/api/v1/events/deployment": {"get": {}}}
    openapi_middleware._remove_deployment_tenant_query_param(paths_missing_post)  # type: ignore[arg-type]
    assert "post" not in paths_missing_post["/api/v1/events/deployment"]

    paths_bad_post: dict[str, object] = {"/api/v1/events/deployment": {"post": []}}
    openapi_middleware._remove_deployment_tenant_query_param(paths_bad_post)  # type: ignore[arg-type]
    assert paths_bad_post["/api/v1/events/deployment"]["post"] == []

    paths_bad_parameters: dict[str, object] = {"/api/v1/events/deployment": {"post": {"parameters": "bad"}}}
    openapi_middleware._remove_deployment_tenant_query_param(paths_bad_parameters)  # type: ignore[arg-type]
    assert paths_bad_parameters["/api/v1/events/deployment"]["post"]["parameters"] == "bad"

    paths_mixed_parameters: dict[str, object] = {
        "/api/v1/events/deployment": {
            "post": {
                "parameters": [
                    "keep",
                    {"name": "tenant_id", "in": "query"},
                ]
            }
        }
    }
    openapi_middleware._remove_deployment_tenant_query_param(paths_mixed_parameters)  # type: ignore[arg-type]
    assert paths_mixed_parameters["/api/v1/events/deployment"]["post"]["parameters"] == ["keep"]

    openapi_middleware._add_analysis_report_result_refs({"components": []})  # type: ignore[arg-type]
    openapi_middleware._add_analysis_report_result_refs({"components": {"schemas": []}})  # type: ignore[arg-type]

    schema_bad_props = {
        "components": {
            "schemas": {
                "AnalyzeJobResultResponse": {"properties": []},
                "AnalyzeReportResponse": {"properties": {"result": {"anyOf": []}}},
            }
        }
    }
    openapi_middleware._add_analysis_report_result_refs(schema_bad_props)  # type: ignore[arg-type]
    assert schema_bad_props["components"]["schemas"]["AnalyzeJobResultResponse"]["properties"] == []

    schema_bad_result = {
        "components": {
            "schemas": {
                "AnalyzeJobResultResponse": {"properties": {"result": []}},
            }
        }
    }
    openapi_middleware._add_analysis_report_result_refs(schema_bad_result)  # type: ignore[arg-type]
    assert schema_bad_result["components"]["schemas"]["AnalyzeJobResultResponse"]["properties"]["result"] == []

    schema_bad_any_of = {
        "components": {
            "schemas": {
                "AnalyzeJobResultResponse": {"properties": {"result": {"anyOf": {}}}},
            }
        }
    }
    openapi_middleware._add_analysis_report_result_refs(schema_bad_any_of)  # type: ignore[arg-type]
    assert schema_bad_any_of["components"]["schemas"]["AnalyzeJobResultResponse"]["properties"]["result"]["anyOf"] == {}

    openapi_middleware._constrain_step_fields({"components": []})  # type: ignore[arg-type]
    openapi_middleware._constrain_step_fields({"components": {"schemas": []}})  # type: ignore[arg-type]

    schema_constrain_branches = {
        "components": {
            "schemas": {
                "NonDictModel": [],
                "BadProperties": {"properties": []},
                "StepNotString": {"properties": {"step": {"type": "integer"}}},
            }
        }
    }
    openapi_middleware._constrain_step_fields(schema_constrain_branches)  # type: ignore[arg-type]
    assert (
        schema_constrain_branches["components"]["schemas"]["StepNotString"]["properties"]["step"]["type"] == "integer"
    )

    schema_with_existing_ref = {
        "components": {
            "schemas": {
                "AnalyzeJobResultResponse": {
                    "properties": {
                        "result": {
                            "anyOf": [
                                {"$ref": "#/components/schemas/AnalysisReport"},
                                {"type": "null"},
                            ]
                        }
                    }
                }
            }
        }
    }
    openapi_middleware._add_analysis_report_result_refs(schema_with_existing_ref)  # type: ignore[arg-type]
    any_of = schema_with_existing_ref["components"]["schemas"]["AnalyzeJobResultResponse"]["properties"]["result"][
        "anyOf"
    ]
    assert any_of.count({"$ref": "#/components/schemas/AnalysisReport"}) == 1
