"""
Test Suite for Store Keys.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
"""

from store import keys


def test_slug_consistency():
    v = "hello"
    s1 = keys._slug(v)
    s2 = keys._slug(v)
    assert s1 == s2
    assert len(s1) == 32
    assert all(c in "0123456789abcdef" for c in s1)


def test_keys_format():
    tid = "tenant"
    assert keys.baseline(tid, "m") == f"bc:{tid}:baseline:{keys._slug('m')}"
    assert keys.weights(tid) == f"bc:{tid}:weights"
    assert keys.granger(tid, "svc").startswith(f"bc:{tid}:granger:")
    assert keys.events(tid) == f"bc:{tid}:events"
