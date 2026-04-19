"""
Test Suite for Granger Causality Storage.

Copyright (c) 2026 Stefan Kumarasinghe.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. See http://www.apache.org/licenses/LICENSE-2.0 for details.
"""

import pytest

from store import granger as gstore


def make_record(cause, effect, strength):
    return {"cause_metric": cause, "effect_metric": effect, "strength": strength}


@pytest.mark.asyncio
async def test_granger_load_and_merge():
    tid = "t1"
    svc = "serviceA"
    assert await gstore.load(tid, svc) == []
    fresh = [
        type(
            "R",
            (),
            {
                "cause_metric": "c1",
                "effect_metric": "e1",
                "strength": 0.5,
                "max_lag": 1,
                "f_statistic": 2,
                "p_value": 0.1,
                "is_causal": True,
            },
        ),
        type(
            "R",
            (),
            {
                "cause_metric": "c2",
                "effect_metric": "e2",
                "strength": 0.2,
                "max_lag": 2,
                "f_statistic": 3,
                "p_value": 0.05,
                "is_causal": False,
            },
        ),
    ]
    merged = await gstore.save_and_merge(tid, svc, fresh)
    assert isinstance(merged, list)
    assert len(merged) == 2
    assert merged[0]["strength"] >= merged[1]["strength"]
    new_rec = type(
        "R",
        (),
        {
            "cause_metric": "c1",
            "effect_metric": "e1",
            "strength": 0.9,
            "max_lag": 1,
            "f_statistic": 2,
            "p_value": 0.1,
            "is_causal": True,
        },
    )
    merged2 = await gstore.save_and_merge(tid, svc, [new_rec])
    assert merged2[0]["strength"] == 0.9
    combined = await gstore.load_all_services(tid, [svc])
    assert combined
