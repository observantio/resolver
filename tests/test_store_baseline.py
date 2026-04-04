"""
Test Suite for Baseline Storage.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
"""

import pytest

from store import baseline as bstore


@pytest.mark.asyncio
async def test_baseline_save_load():
    tid = "ten1"
    metric = "mymetric"
    from engine.baseline.compute import Baseline

    base = Baseline(mean=1.0, std=1.0, lower=-2.0, upper=4.0)
    await bstore.save(tid, metric, base)
    loaded = await bstore.load(tid, metric)
    assert loaded is not None
    assert loaded.mean == base.mean
    base2 = Baseline(mean=2.0, std=0.5, lower=-1.0, upper=3.0)
    await bstore.save(tid, metric, base2)
    l2 = await bstore.load(tid, metric)
    assert l2.mean == 2.0
    ts = [0.0, 1.0, 2.0, 3.0, 4.0]
    vals = [1.0, 2.0, 1.5, 2.5, 1.0]
    result = await bstore.compute_and_persist(tid, metric, ts, vals)
    assert hasattr(result, "mean")
