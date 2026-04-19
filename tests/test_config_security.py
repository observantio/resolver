"""
Configuration security tests for Resolver strict production controls.

Copyright (c) 2026 Stefan Kumarasinghe.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. See http://www.apache.org/licenses/LICENSE-2.0 for details.
"""

from __future__ import annotations

import importlib
import os
import sys
import types
from unittest.mock import patch

import pytest


def _reload_config_module() -> types.ModuleType:
    for module_name in ("config", "Resolvers.config"):
        if module_name in sys.modules:
            del sys.modules[module_name]
    return importlib.import_module("config")


def _base_production_env() -> dict[str, str]:
    return {
        "APP_ENV": "production",
        "RESOLVER_DATABASE_URL": "postgresql://safeuser:safePass_123@db:5432/resolver",
        "RESOLVER_EXPECTED_SERVICE_TOKEN": "resolver_expected_service_token_prod_12345",
        "RESOLVER_CONTEXT_VERIFY_KEY": "resolver_context_verify_key_prod_1234567890",
        "RESOLVER_CONTEXT_ISSUER": "watchdog-main",
        "RESOLVER_CONTEXT_AUDIENCE": "resolver",
        "RESOLVER_CONTEXT_ALGORITHMS": "HS256",
        "RESOLVER_CONTEXT_REPLAY_TTL_SECONDS": "180",
    }


def test_rejects_invalid_context_algorithm():
    with patch.dict(os.environ, {"RESOLVER_CONTEXT_ALGORITHMS": "RS256"}, clear=False), pytest.raises(ValueError):
        _reload_config_module()


def test_production_rejects_missing_expected_service_token():
    env = _base_production_env()
    env["RESOLVER_EXPECTED_SERVICE_TOKEN"] = ""
    with patch.dict(os.environ, env, clear=False), pytest.raises(ValueError):
        _reload_config_module()


def test_production_rejects_weak_context_verify_key():
    env = _base_production_env()
    env["RESOLVER_CONTEXT_VERIFY_KEY"] = "changeme"
    with patch.dict(os.environ, env, clear=False), pytest.raises(ValueError):
        _reload_config_module()


def test_production_accepts_strong_security_config():
    with patch.dict(os.environ, _base_production_env(), clear=False):
        module = _reload_config_module()
    assert module.settings.expected_service_token.startswith("resolver_expected_service_token")
    assert module.settings.context_replay_ttl_seconds == 180
