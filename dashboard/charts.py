"""Helper utilities to build dashboard chart payloads.

The dashboard intentionally works with serialisable dictionaries so that the
same functions can serve server-side rendered templates and modern front-end
frameworks alike.  The consuming layer is expected to understand a Chart.js-like
configuration which keeps the returned payloads lightweight yet expressive.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, List, Mapping, Sequence

from analytics.flake_metrics import WeeklyFlakeInsight


@dataclass(frozen=True)
class TrendPoint:
    """Time series point for the flake-rate trend chart."""

    label: str
    value: float


def build_flake_rate_line_chart(insights: Sequence[WeeklyFlakeInsight]) -> Dict[str, object]:
    """Return a serialisable configuration for the weekly flake-rate trend.

    The resulting dictionary follows a structure that is compatible with
    Chart.js line charts which are supported by most dashboard stacks.  The
    caller is responsible for formatting the ``label`` (for example converting
    a ``date`` into an ISO string).
    """

    labels: List[str] = []
    data_points: List[float] = []
    anomalies: List[int] = []

    for insight in insights:
        labels.append(insight.week_start.isoformat())
        data_points.append(round(insight.flake_rate * 100, 4))
        anomalies.append(1 if insight.is_anomalous else 0)

    return {
        "type": "line",
        "data": {
            "labels": labels,
            "datasets": [
                {
                    "label": "Weekly flake rate (%)",
                    "data": data_points,
                    "borderColor": "#2563eb",
                    "backgroundColor": "rgba(37,99,235,0.2)",
                    "tension": 0.35,
                },
                {
                    "label": "Anomaly",
                    "data": anomalies,
                    "type": "bar",
                    "backgroundColor": "rgba(220,38,38,0.4)",
                    "yAxisID": "anomalies",
                },
            ],
        },
        "options": {
            "interaction": {"mode": "index", "intersect": False},
            "scales": {
                "y": {
                    "title": {"display": True, "text": "Flake rate (%)"},
                    "beginAtZero": True,
                },
                "anomalies": {
                    "position": "right",
                    "display": False,
                    "min": 0,
                    "max": 1,
                },
            },
            "plugins": {
                "tooltip": {
                    "callbacks": {
                        "label": "function(ctx) { return ctx.datasetIndex === 0 ? ctx.parsed.y + '% flake' : 'Anomaly'; }",
                    }
                }
            },
        },
    }


@dataclass(frozen=True)
class CohortSeries:
    """Represents a cohort breakdown for the cohort analysis tabs."""

    cohort: str
    subgroup: str
    flake_rate: float


def build_cohort_tabs(data: Mapping[str, Iterable[CohortSeries]]) -> List[Dict[str, object]]:
    """Produce tab metadata for cohort analysis views.

    Args:
        data: Mapping of cohort name (for example ``"Device OS"``) to a list of
            :class:`CohortSeries` rows.

    Returns:
        A list of dictionaries representing the tabs.  Each tab contains a bar
        chart configuration that can be directly rendered by Chart.js or a
        similar library.
    """

    tabs: List[Dict[str, object]] = []

    for cohort_name, rows in data.items():
        labels: List[str] = []
        values: List[float] = []

        for row in rows:
            labels.append(row.subgroup)
            values.append(round(row.flake_rate * 100, 4))

        tabs.append(
            {
                "title": cohort_name,
                "chart": {
                    "type": "bar",
                    "data": {
                        "labels": labels,
                        "datasets": [
                            {
                                "label": f"{cohort_name} flake rate (%)",
                                "data": values,
                                "backgroundColor": "rgba(22,163,74,0.4)",
                                "borderColor": "#16a34a",
                            }
                        ],
                    },
                    "options": {
                        "indexAxis": "y" if len(labels) > 5 else "x",
                        "scales": {
                            "x": {
                                "beginAtZero": True,
                                "title": {"display": True, "text": "Flake rate (%)"},
                            }
                        },
                    },
                },
            }
        )

    return tabs
