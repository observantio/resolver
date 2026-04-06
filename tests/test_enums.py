"""
Test cases for enums used in the analysis engine, including Severity, Signal, ChangeType, and RcaCategory, validating
their properties and relationships.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
"""

from engine.enums import ChangeType, RcaCategory, Severity, Signal


def test_severity_from_score_and_weight():
    assert Severity.from_score(0.8) == Severity.CRITICAL
    assert Severity.from_score(0.5) == Severity.HIGH
    assert Severity.from_score(0.3) == Severity.MEDIUM
    assert Severity.from_score(0.1) == Severity.LOW
    assert Severity.LOW.weight() < Severity.MEDIUM.weight() < Severity.HIGH.weight() < Severity.CRITICAL.weight()


def test_signal_enum():
    assert list(Signal) == [Signal.METRICS, Signal.LOGS, Signal.TRACES, Signal.EVENTS]


def test_change_type_and_rca_category():
    assert ChangeType.SPIKE.value == "spike"
    assert RcaCategory.DEPLOYMENT.value == "deployment"
