"""
Bayesian scoring logic for root cause analysis, providing functionality to compute the posterior probability of
different root cause categories based on observed evidence (such as deployment events, metric spikes, log bursts,
latency spikes, and error propagation) using configurable priors and likelihoods, to assist in prioritizing potential
causes during incident investigation.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
"""

from __future__ import annotations

from dataclasses import dataclass

from config import settings
from engine.enums import RcaCategory


def _configured_priors() -> dict[RcaCategory, float]:
    return {RcaCategory(k): v for k, v in settings.bayesian_priors.items()}


def _configured_likelihoods() -> dict[RcaCategory, dict[str, float]]:
    return {RcaCategory(k): v for k, v in settings.bayesian_likelihoods.items()}


@dataclass(frozen=True)
class BayesianScore:
    category: RcaCategory
    posterior: float
    prior: float
    likelihood: float


def score(
    has_deployment_event: bool,
    has_metric_spike: bool,
    has_log_burst: bool,
    has_latency_spike: bool,
    has_error_propagation: bool,
) -> list[BayesianScore]:
    evidence: dict[str, bool] = {
        "has_deployment_event": has_deployment_event,
        "has_metric_spike": has_metric_spike,
        "has_log_burst": has_log_burst,
        "has_latency_spike": has_latency_spike,
        "has_error_propagation": has_error_propagation,
    }

    priors = _configured_priors()
    likelihood_map = _configured_likelihoods()

    raw_posteriors: dict[RcaCategory, float] = {}
    for category, prior in priors.items():
        likelihood = 1.0
        likelihoods = likelihood_map.get(category, {})
        for feature, present in evidence.items():
            p = likelihoods.get(feature, settings.bayesian_default_feature_prob)
            likelihood *= p if present else (1.0 - p)
        raw_posteriors[category] = prior * likelihood

    total = sum(raw_posteriors.values()) or 1.0
    results = [
        BayesianScore(
            category=cat,
            posterior=round(raw / total, 4),
            prior=round(priors.get(cat, 0.0), 4),
            likelihood=round(raw / priors.get(cat, 1.0) if priors.get(cat) else 0.0, 4),
        )
        for cat, raw in raw_posteriors.items()
    ]
    return sorted(results, key=lambda s: s.posterior, reverse=True)
