"""
Loki connector tests focused on validating that query normalization correctly handles empty and empty-compatible
matchers, ensuring that queries are transformed into a format that Loki can process without errors.

Copyright (c) 2026 Stefan Kumarasinghe.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. See http://www.apache.org/licenses/LICENSE-2.0 for details.
"""

from connectors.loki import LokiConnector


def test_loki_normalizes_empty_and_empty_compatible_matchers():
    assert LokiConnector._normalize_query("{}") == '{service=~".+"}'
    assert LokiConnector._normalize_query("") == '{service=~".+"}'
    assert LokiConnector._normalize_query('{app=~".*"}') == '{app=~".+"}'
    assert LokiConnector._normalize_query('{service=~".+"}') == '{service=~".+"}'
