"""
OpenAPI customization wiring for the Resolver FastAPI app.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
"""

from __future__ import annotations

from http import HTTPStatus
import re
from typing import Any

from fastapi import FastAPI
from fastapi.openapi.utils import get_openapi

JSON_SCHEMA_DIALECT = "https://json-schema.org/draft/2020-12/schema"
_METHOD_ACTIONS: dict[str, str] = {
    "GET": "Retrieve",
    "POST": "Create",
    "PUT": "Replace",
    "PATCH": "Update",
    "DELETE": "Delete",
}
_UNAUTHORIZED_PATH = "/api/v1/ready"
_GENERIC_DESCRIPTION_PATTERN = re.compile(r"^Handles [A-Z]+ requests for `/.+`\.$")
_NON_OPERATION_KEYS = {"summary", "description", "parameters", "servers"}
_HTTP_METHODS = {"get", "post", "put", "patch", "delete", "options", "head", "trace"}
_STEP_PATTERN = r"^[1-9][0-9]*[smhd]$"
_STANDARD_ERROR_CODES = {"400", "401", "403", "404", "429", "500", "502", "503"}


def _status_description(status_code: int) -> str:
    try:
        return HTTPStatus(status_code).phrase
    except ValueError:
        return f"HTTP {status_code}"


def _summary_from_operation(operation: dict[str, Any], method: str, path: str) -> str:
    operation_id = operation.get("operationId")
    if isinstance(operation_id, str) and operation_id.strip():
        return operation_id.replace("_", " ").strip().title()
    action = _METHOD_ACTIONS.get(method.upper(), method.upper())
    resource = path.strip("/").split("/")[-1] if path.strip("/") else "root"
    resource = resource.split(":")[0].replace("{", "").replace("}", "").replace("_", " ").replace("-", " ").strip()
    resource = resource or "resource"
    return f"{action} {resource.title()}"


def _ensure_security_schemes(schema: dict[str, Any]) -> None:
    components = schema.setdefault("components", {})
    if not isinstance(components, dict):
        return
    security_schemes = components.setdefault("securitySchemes", {})
    if not isinstance(security_schemes, dict):
        return
    security_schemes.setdefault(
        "ServiceToken",
        {
            "type": "apiKey",
            "in": "header",
            "name": "x-service-token",
            "description": "Internal service token required for resolver API access.",
        },
    )
    security_schemes.setdefault(
        "ContextBearer",
        {
            "type": "http",
            "scheme": "bearer",
            "bearerFormat": "JWT",
            "description": "Context JWT carrying tenant and user scope for internal calls.",
        },
    )


def _ensure_error_schema(schema: dict[str, Any]) -> None:
    components = schema.setdefault("components", {})
    if not isinstance(components, dict):
        return
    schemas = components.setdefault("schemas", {})
    if not isinstance(schemas, dict):
        return
    schemas.setdefault(
        "ErrorResponse",
        {
            "type": "object",
            "properties": {"detail": {"type": "string", "title": "Detail"}},
            "required": ["detail"],
            "title": "ErrorResponse",
        },
    )


def _apply_operation_security(path: str, operation: dict[str, Any]) -> None:
    if not path.startswith("/api/v1") or path == _UNAUTHORIZED_PATH:
        return
    security = operation.get("security")
    if isinstance(security, list) and len(security) > 0:
        return
    operation["security"] = [{"ServiceToken": [], "ContextBearer": []}]


def _apply_inferred_responses(path: str, method: str, operation: dict[str, Any]) -> None:
    responses = operation.setdefault("responses", {})
    if not isinstance(responses, dict):
        return

    if path.startswith("/api/v1") and path != _UNAUTHORIZED_PATH:
        responses.setdefault("401", {"description": "Unauthorized"})
        responses.setdefault("403", {"description": "Forbidden"})

    if "requestBody" in operation or method.upper() in {"POST", "PUT", "PATCH", "DELETE"}:
        responses.setdefault("400", {"description": "Bad Request"})

    if "{" in path and "}" in path:
        responses.setdefault("404", {"description": "Not Found"})

    if path == _UNAUTHORIZED_PATH:
        responses.setdefault("503", {"description": "Service Unavailable"})


def _ensure_standard_error_content(operation: dict[str, Any]) -> None:
    responses = operation.get("responses")
    if not isinstance(responses, dict):
        return

    for code, response in responses.items():
        if not isinstance(response, dict):
            continue
        if str(code) not in _STANDARD_ERROR_CODES:
            continue
        if isinstance(response.get("content"), dict):
            continue
        response["content"] = {
            "application/json": {"schema": {"$ref": "#/components/schemas/ErrorResponse"}}
        }


def _ensure_operation_docs(path: str, method: str, operation: dict[str, Any]) -> None:
    if not isinstance(operation.get("summary"), str) or not operation.get("summary", "").strip():
        operation["summary"] = _summary_from_operation(operation, method, path)

    description = operation.get("description")
    if isinstance(description, str):
        if _GENERIC_DESCRIPTION_PATTERN.match(description.strip()):
            operation.pop("description", None)
    elif description is not None:
        operation.pop("description", None)

    responses = operation.get("responses")
    if not isinstance(responses, dict):
        return
    for code, response in responses.items():
        if not isinstance(response, dict):
            continue
        if not str(code).startswith("2"):
            continue
        current = response.get("description")
        if isinstance(current, str) and current.strip() and current.strip() != "Successful Response":
            continue
        response["description"] = _status_description(int(str(code))) if str(code).isdigit() else "Success"


def _iter_method_operations(paths: dict[str, Any]) -> list[tuple[str, str, dict[str, Any]]]:
    ops: list[tuple[str, str, dict[str, Any]]] = []
    for path, path_item in paths.items():
        if not isinstance(path_item, dict):
            continue
        for method, operation in path_item.items():
            if method in _NON_OPERATION_KEYS or method.lower() not in _HTTP_METHODS:
                continue
            if not isinstance(operation, dict):
                continue
            ops.append((path, method.lower(), operation))
    return ops


def _remove_deployment_tenant_query_param(paths: dict[str, Any]) -> None:
    path_item = paths.get("/api/v1/events/deployment")
    if not isinstance(path_item, dict):
        return
    post = path_item.get("post")
    if not isinstance(post, dict):
        return
    parameters = post.get("parameters")
    if not isinstance(parameters, list):
        return

    filtered = []
    for param in parameters:
        if not isinstance(param, dict):
            filtered.append(param)
            continue
        if param.get("in") == "query" and param.get("name") == "tenant_id":
            continue
        filtered.append(param)
    post["parameters"] = filtered


def _add_analysis_report_result_refs(schema: dict[str, Any]) -> None:
    components = schema.get("components")
    if not isinstance(components, dict):
        return
    schemas = components.get("schemas")
    if not isinstance(schemas, dict):
        return
    for model_name in ("AnalyzeJobResultResponse", "AnalyzeReportResponse"):
        model = schemas.get(model_name)
        if not isinstance(model, dict):
            continue
        properties = model.get("properties")
        if not isinstance(properties, dict):
            continue
        result = properties.get("result")
        if not isinstance(result, dict):
            continue
        any_of = result.get("anyOf")
        if not isinstance(any_of, list):
            continue
        report_ref = {"$ref": "#/components/schemas/AnalysisReport"}
        if report_ref not in any_of:
            any_of.insert(0, report_ref)


def _constrain_step_fields(schema: dict[str, Any]) -> None:
    components = schema.get("components")
    if not isinstance(components, dict):
        return
    schemas = components.get("schemas")
    if not isinstance(schemas, dict):
        return

    for model in schemas.values():
        if not isinstance(model, dict):
            continue
        properties = model.get("properties")
        if not isinstance(properties, dict):
            continue
        step = properties.get("step")
        if not isinstance(step, dict):
            continue
        if step.get("type") != "string":
            continue
        step.setdefault(
            "pattern",
            _STEP_PATTERN,
        )


def install_custom_openapi(app: FastAPI) -> None:
    def custom_openapi() -> Any:
        if app.openapi_schema:
            return app.openapi_schema

        schema_value: Any = get_openapi(
            title=app.title,
            version=app.version,
            description=app.description,
            routes=app.routes,
        )
        if not isinstance(schema_value, dict):
            return schema_value
        schema = schema_value

        _ensure_security_schemes(schema)
        _ensure_error_schema(schema)

        paths = schema.get("paths")
        if isinstance(paths, dict):
            _remove_deployment_tenant_query_param(paths)
            for path, method, operation in _iter_method_operations(paths):
                _apply_operation_security(path, operation)
                _apply_inferred_responses(path, method, operation)
                _ensure_standard_error_content(operation)
                _ensure_operation_docs(path, method, operation)
        _add_analysis_report_result_refs(schema)
        _constrain_step_fields(schema)

        schema["jsonSchemaDialect"] = JSON_SCHEMA_DIALECT
        app.openapi_schema = schema
        return schema

    app.openapi = custom_openapi  # type: ignore[method-assign]
