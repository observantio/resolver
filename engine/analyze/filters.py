"""
Analyzer Filter Helpers.

Copyright (c) 2026 Stefan Kumarasinghe.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. See http://www.apache.org/licenses/LICENSE-2.0 for details.
"""

from __future__ import annotations

_SERVICE_LABEL_KEYS = ("service", "service_name", "service.name", "job")


def normalize_services(services: list[str] | None) -> set[str]:
    return {str(service or "").strip().lower() for service in (services or []) if str(service or "").strip()}


def result_matches_services(result: object, services: set[str]) -> bool:
    if not services:
        return True
    if not isinstance(result, dict):
        return False
    metric = result.get("metric", {})
    if not isinstance(metric, dict):
        return False
    for key in _SERVICE_LABEL_KEYS:
        value = str(metric.get(key) or "").strip().lower()
        if value and value in services:
            return True
    return False


def filter_metric_response_by_services(response: object, services: set[str]) -> object:
    filtered_response = response
    if services and isinstance(response, dict):
        data = response.get("data")
        if isinstance(data, dict):
            results = data.get("result")
            if isinstance(results, list):
                filtered = [item for item in results if result_matches_services(item, services)]
                if len(filtered) != len(results):
                    response_copy = dict(response)
                    data_copy = dict(data)
                    data_copy["result"] = filtered
                    response_copy["data"] = data_copy
                    filtered_response = response_copy
    return filtered_response
