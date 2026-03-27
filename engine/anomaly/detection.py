"""
Detection logic for identifying anomalies in time series metric data using a combination of statistical methods (z-score, MAD) and machine learning (Isolation Forest), along with heuristics for classifying the type and severity of detected anomalies, to provide actionable insights into potential issues in monitored systems.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
"""

from __future__ import annotations

from importlib import import_module
import math
from typing import Callable, List, Protocol

import numpy as np

from engine.enums import ChangeType, Severity
from api.responses import MetricAnomaly
from config import settings

linregress: Callable[[np.ndarray, np.ndarray], tuple[float, float, float, float, float]] = import_module("scipy.stats").linregress


class IsolationForestModel(Protocol):
    def fit_predict(self, data: np.ndarray) -> np.ndarray: ...
    def score_samples(self, data: np.ndarray) -> np.ndarray: ...


class IsolationForestFactory(Protocol):
    def __call__(
        self,
        *,
        contamination: float,
        random_state: int,
        n_estimators: int,
    ) -> IsolationForestModel: ...


IsolationForest: IsolationForestFactory = import_module("sklearn.ensemble").IsolationForest


def _mad_scores(arr: np.ndarray) -> np.ndarray:
    median = np.median(arr)
    mad = np.median(np.abs(arr - median))
    if mad == 0:
        return np.zeros_like(arr)
    return np.asarray(settings.anomaly_mad_scale * (arr - median) / mad, dtype=float)


def _series_median_iqr(arr: np.ndarray) -> tuple[float, float, float, float]:
    med = float(np.median(arr))
    q1 = float(np.percentile(arr, 25))
    q3 = float(np.percentile(arr, 75))
    iqr = float(q3 - q1)
    return med, q1, q3, iqr


def _iqr_score_value(value: float, median: float, iqr: float) -> float:
    if iqr <= 0:
        return 0.0
    return float((value - median) / iqr)


def _tukey_outlier_class(
    value: float,
    q1: float,
    q3: float,
    iqr: float,
) -> str:
    if iqr <= 0:
        return "none"
    mild_k = float(getattr(settings, "tukey_mild_k", 1.5))
    extreme_k = float(getattr(settings, "tukey_extreme_k", 3.0))
    if value > q3 + extreme_k * iqr:
        return "extreme_high"
    if value > q3 + mild_k * iqr:
        return "mild_high"
    if value < q1 - extreme_k * iqr:
        return "extreme_low"
    if value < q1 - mild_k * iqr:
        return "mild_low"
    return "none"


def _cusum_changepoints(arr: np.ndarray, threshold: float | None = None) -> np.ndarray:
    if threshold is None:
        threshold = settings.cusum_threshold

    mu, sigma = arr.mean(), arr.std()
    if sigma == 0:
        return np.zeros(len(arr), dtype=bool)
    normed = (arr - mu) / sigma
    cusum_pos = np.zeros(len(arr))
    cusum_neg = np.zeros(len(arr))
    k = settings.anomaly_cusum_k
    for i in range(1, len(arr)):
        cusum_pos[i] = max(0, cusum_pos[i-1] + normed[i] - k)
        cusum_neg[i] = max(0, cusum_neg[i-1] - normed[i] - k)
    return (cusum_pos > threshold) | (cusum_neg > threshold)


def _change_type(value: float, mean: float, z: float, trend_slope: float) -> ChangeType:
    _ = (value, mean)
    if abs(trend_slope) > settings.anomaly_drift_slope_threshold:
        return ChangeType.drift
    if z > 0:
        return ChangeType.spike
    if z < 0:
        return ChangeType.drop
    return ChangeType.shift


def _severity(z: float, mad: float, iso: int, iqr_score: float = 0.0) -> Severity:
    score = 0.0
    az = abs(z)
    for threshold, weight in settings.anomaly_z_thresholds:
        if az >= threshold:
            score += weight
            break
    am = abs(mad)
    for threshold, weight in settings.anomaly_mad_thresholds:
        if am >= threshold:
            score += weight
            break
    ai = abs(iqr_score)
    for threshold, weight in settings.anomaly_iqr_score_thresholds:
        if ai >= threshold:
            score += weight
            break
    if iso == -1:
        score += settings.anomaly_iso_weight
    return Severity.from_score(min(score, 1.0))


def _is_precision_profile() -> bool:
    return str(getattr(settings, "quality_gating_profile", "")).strip().lower().startswith("precision")


def _apply_density_cap(anomalies: List[MetricAnomaly], timestamps: np.ndarray) -> List[MetricAnomaly]:
    if not anomalies:
        return anomalies
    max_density = float(getattr(settings, "quality_max_anomaly_density_per_metric_per_hour", 0.0))
    if max_density <= 0:
        return anomalies

    if timestamps.size >= 2:
        window_seconds = max(1.0, float(timestamps.max() - timestamps.min()))
    else:
        window_seconds = 3600.0
    window_hours = max(window_seconds / 3600.0, 1.0 / 60.0)
    keep_limit = max(1, int(math.ceil(max_density * window_hours)))
    if len(anomalies) <= keep_limit:
        return anomalies

    ranked = sorted(
        anomalies,
        key=lambda a: (
            a.severity.weight(),
            abs(float(a.z_score)),
            abs(float(a.mad_score)),
            abs(float(a.iqr_score)),
        ),
        reverse=True,
    )
    kept = ranked[:keep_limit]
    return sorted(kept, key=lambda a: a.timestamp)


def _compress_runs(anomalies: List[MetricAnomaly]) -> List[MetricAnomaly]:
    if not anomalies or len(anomalies) <= settings.anomaly_run_keep_max:
        return anomalies

    sorted_items = sorted(anomalies, key=lambda a: a.timestamp)
    diffs = [
        sorted_items[i].timestamp - sorted_items[i - 1].timestamp
        for i in range(1, len(sorted_items))
        if sorted_items[i].timestamp > sorted_items[i - 1].timestamp
    ]
    typical_step = float(np.median(diffs)) if diffs else 0.0
    max_gap = (typical_step * float(settings.anomaly_run_gap_multiplier)) if typical_step > 0 else 0.0

    groups: list[list[MetricAnomaly]] = []
    current: list[MetricAnomaly] = [sorted_items[0]]
    for item in sorted_items[1:]:
        prev = current[-1]
        same_type = item.change_type == prev.change_type
        close_in_time = (max_gap <= 0.0) or ((item.timestamp - prev.timestamp) <= max_gap)
        if same_type and close_in_time:
            current.append(item)
        else:
            groups.append(current)
            current = [item]
    groups.append(current)

    keep_max = max(1, int(settings.anomaly_run_keep_max))
    compressed: list[MetricAnomaly] = []
    for group in groups:
        if len(group) <= keep_max:
            compressed.extend(group)
            continue

        strongest = max(
            group,
            key=lambda a: (abs(a.z_score), abs(a.mad_score), abs(a.iqr_score), a.severity.weight()),
        )
        selected: list[MetricAnomaly] = [group[0], strongest, group[-1]]
        uniq = {}
        for item in selected:
            key = (item.timestamp, item.value, item.change_type.value)
            uniq[key] = item
        ranked = sorted(
            uniq.values(),
            key=lambda a: (a.timestamp, -abs(a.z_score)),
        )
        compressed.extend(ranked[:keep_max])

    return sorted(compressed, key=lambda a: a.timestamp)


def detect(
    metric: str,
    timestamps: List[float],
    values: List[float],
    sensitivity: float | None = None,
) -> List[MetricAnomaly]:
    if len(values) < settings.min_samples:
        return []

    if sensitivity is None:
        sensitivity = settings.anomaly_default_sensitivity

    contamination = max(
        settings.anomaly_contamination_min,
        min(
            settings.anomaly_contamination_max,
            settings.anomaly_contamination_divisor
            / max(sensitivity, settings.anomaly_min_sensitivity),
        ),
    )
    if _is_precision_profile():
        contamination = max(
            settings.anomaly_contamination_min,
            min(0.10, contamination * 0.35),
        )

    arr_raw = np.array(values, dtype=float)
    ts_raw = np.array(timestamps, dtype=float)
    finite = np.isfinite(arr_raw) & np.isfinite(ts_raw)
    if finite.sum() < settings.min_samples:
        return []

    arr = arr_raw[finite]
    ts = ts_raw[finite]
    clean = arr
    mean, std = clean.mean(), clean.std()
    if std == 0:
        return []

    z_scores = (arr - mean) / std
    mad_scores = _mad_scores(arr)
    med, q1, q3, iqr = _series_median_iqr(clean)
    cusum_flags = _cusum_changepoints(arr)
    p5 = float(np.percentile(clean, settings.anomaly_percentile_low))
    p95 = float(np.percentile(clean, settings.anomaly_percentile_high))

    iso = IsolationForest(
        contamination=contamination,
        random_state=settings.anomaly_iso_random_state,
        n_estimators=settings.anomaly_iso_n_estimators,
    )
    iso_labels = iso.fit_predict(arr.reshape(-1, 1))
    iso_scores = iso.score_samples(arr.reshape(-1, 1))

    slope, *_ = linregress(np.arange(len(clean)), clean)

    anomalies: List[MetricAnomaly] = []
    for t, v, z, m, c, iso_l, iso_s in (
        zip(ts, arr, z_scores, mad_scores, cusum_flags, iso_labels, iso_scores)
    ):
        iq = _iqr_score_value(float(v), med, iqr)
        tukey = _tukey_outlier_class(float(v), q1, q3, iqr)
        iqr_signal = tukey != "none"
        stat_flag = (
            abs(z) >= settings.zscore_threshold
            or abs(m) >= settings.mad_threshold
            or c
            or iqr_signal
        )
        iso_flag = (
            iso_l == -1
            and (
                abs(z) >= settings.zscore_threshold * 0.7
                or abs(m) >= settings.mad_threshold * 0.7
                or abs(iq) >= settings.mad_threshold * 0.5
            )
        )
        flagged = stat_flag or iso_flag
        if not flagged:
            continue

        sev = _severity(z, m, iso_l, iq)
        ctype = _change_type(v, mean, z, slope)

        anomalies.append(MetricAnomaly(
            metric_name=metric,
            timestamp=float(t),
            value=float(v),
            change_type=ctype,
            z_score=round(float(z), 3),
            mad_score=round(float(m), 3),
            isolation_score=round(float(iso_s), 4),
            expected_range=(round(p5, 4), round(p95, 4)),
            severity=sev,
            iqr_score=round(float(iq), 3),
            tukey_outlier_class=tukey,
            description=(
                f"{metric}: {ctype.value} of {v:.4g} "
                f"(z={z:+.1f}, MAD={m:+.1f}, IQR={iq:+.2f}, Tukey={tukey}, "
                f"expected=[{p5:.4g}, {p95:.4g}])"
            ),
        ))

    if bool(settings.anomaly_compress_runs):
        anomalies = _compress_runs(anomalies)
    return _apply_density_cap(anomalies, ts)
