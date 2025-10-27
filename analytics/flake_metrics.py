"""Utilities for computing flake-rate metrics and anomalies.

This module focuses on aggregating raw flake samples into week level metrics
and augmenting those metrics with actionable context such as week-over-week
changes and anomaly scores.  The functions avoid any heavy dependencies so
that they can be reused by batch jobs as well as web handlers.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date
from statistics import mean, pstdev
from typing import Iterable, List, Optional, Sequence


@dataclass(frozen=True)
class WeeklyFlakeSample:
    """Raw flake statistics for a calendar week.

    Attributes:
        week_start: The Monday associated with this week.  Using a ``date``
            keeps the data structure compact and portable when serialising.
        test_runs: Total number of runs executed during the week.
        flake_failures: Number of flaky failures observed during the week.
    """

    week_start: date
    test_runs: int
    flake_failures: int

    @property
    def flake_rate(self) -> float:
        """Return the ratio of flaky failures to total runs.

        The value is expressed as a floating point ratio between ``0`` and ``1``.
        When there are no test runs we treat the flake rate as ``0`` to avoid a
        divide-by-zero error.  Such a scenario typically signals a pipeline
        outage and downstream consumers can still use the volume columns to
        reason about the data.
        """

        if self.test_runs == 0:
            return 0.0
        return self.flake_failures / self.test_runs


@dataclass(frozen=True)
class WeeklyFlakeInsight:
    """Computed metrics for a week including deltas and anomalies."""

    week_start: date
    test_runs: int
    flake_failures: int
    flake_rate: float
    wow_delta: Optional[float]
    z_score: Optional[float]
    is_anomalous: bool


def _sorted(samples: Iterable[WeeklyFlakeSample]) -> List[WeeklyFlakeSample]:
    return sorted(samples, key=lambda sample: sample.week_start)


def _compute_z_score(value: float, history: Sequence[float]) -> Optional[float]:
    """Return the Z-score of ``value`` relative to ``history``.

    ``None`` is returned when the history is too small or has no variance.
    """

    if len(history) < 2:
        return None

    variance = pstdev(history)
    if math.isclose(variance, 0.0, abs_tol=1e-12):
        return None

    hist_mean = mean(history)
    return (value - hist_mean) / variance


def enrich_with_insights(
    samples: Iterable[WeeklyFlakeSample],
    anomaly_threshold: float = 2.5,
) -> List[WeeklyFlakeInsight]:
    """Augment weekly samples with week-over-week deltas and Z-scores.

    Args:
        samples: Iterable of :class:`WeeklyFlakeSample` items.  They can be
            provided in any order; the function will sort them chronologically
            before computing deltas.
        anomaly_threshold: Absolute Z-score after which a week is considered an
            anomaly.  The default of ``2.5`` offers a balanced sensitivity for
            thin datasets.  Adjust the value if you have more historical data
            and prefer a stricter threshold.

    Returns:
        A list of :class:`WeeklyFlakeInsight` sorted by week start.
    """

    ordered = _sorted(samples)
    insights: List[WeeklyFlakeInsight] = []
    history_rates: List[float] = []

    previous_rate: Optional[float] = None

    for sample in ordered:
        rate = sample.flake_rate
        wow_delta = None
        if previous_rate is not None:
            if math.isclose(previous_rate, 0.0, abs_tol=1e-12):
                wow_delta = math.inf if rate > 0 else 0.0
            else:
                wow_delta = (rate - previous_rate) / previous_rate

        z_score = _compute_z_score(rate, history_rates)
        is_anomalous = z_score is not None and abs(z_score) >= anomaly_threshold

        insights.append(
            WeeklyFlakeInsight(
                week_start=sample.week_start,
                test_runs=sample.test_runs,
                flake_failures=sample.flake_failures,
                flake_rate=rate,
                wow_delta=wow_delta,
                z_score=z_score,
                is_anomalous=is_anomalous,
            )
        )

        history_rates.append(rate)
        previous_rate = rate

    return insights


def latest_anomalies(
    insights: Sequence[WeeklyFlakeInsight],
    limit: int = 5,
) -> List[WeeklyFlakeInsight]:
    """Return the most recent anomalous weeks.

    ``limit`` guards against overwhelming alert payloads while keeping the
    function deterministic for unit testing.
    """

    anomalies = [item for item in insights if item.is_anomalous]
    anomalies.sort(key=lambda item: item.week_start, reverse=True)
    return anomalies[:limit]
