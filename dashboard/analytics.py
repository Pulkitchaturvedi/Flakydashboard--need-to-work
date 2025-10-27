"""Helper functions for aggregating flaky test analytics."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Optional

import pandas as pd


@dataclass
class FilterSelections:
    platforms: Optional[Iterable[str]] = None
    teams: Optional[Iterable[str]] = None
    pipelines: Optional[Iterable[str]] = None
    app_versions: Optional[Iterable[str]] = None
    date_range: Optional[tuple[pd.Timestamp, pd.Timestamp]] = None


def apply_filters(df: pd.DataFrame, selections: FilterSelections) -> pd.DataFrame:
    filtered = df.copy()

    if selections.platforms:
        filtered = filtered[filtered["platform"].isin(selections.platforms)]

    if selections.teams:
        filtered = filtered[filtered["team"].isin(selections.teams)]

    if selections.pipelines:
        filtered = filtered[filtered["pipeline"].isin(selections.pipelines)]

    if selections.app_versions:
        filtered = filtered[filtered["app_version"].isin(selections.app_versions)]

    if selections.date_range:
        start, end = selections.date_range
        date_column = "event_date" if "event_date" in filtered.columns else "test_date"
        if date_column in filtered.columns:
            filtered = filtered[
                (filtered[date_column] >= start) & (filtered[date_column] <= end)
            ]
    return filtered


def compute_total_flaky_tests(df: pd.DataFrame) -> int:
    if "test_id" in df.columns:
        return int(df["test_id"].nunique())
    if "test_name" in df.columns:
        return int(df["test_name"].nunique())
    return int(len(df))


def compute_unique_root_causes(df: pd.DataFrame) -> int:
    column = "root_cause" if "root_cause" in df.columns else "root_cause_summary"
    if column in df.columns:
        return int(df[column].dropna().nunique())
    return 0


def compute_failure_rate_trend(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["date", "failure_rate"])

    date_column = "event_date" if "event_date" in df.columns else "test_date"
    working = df.copy()
    if date_column not in working.columns:
        working[date_column] = pd.Timestamp.utcnow().normalize()

    if "failure_rate" in working.columns:
        grouped = (
            working[[date_column, "failure_rate"]]
            .groupby(date_column, as_index=False)
            .mean()
        )
        grouped.rename(columns={date_column: "date"}, inplace=True)
        return grouped

    failure_count_col = "failure_count" if "failure_count" in working.columns else None
    total_runs_col = "total_runs" if "total_runs" in working.columns else None

    if failure_count_col and total_runs_col:
        grouped = (
            working[[date_column, failure_count_col, total_runs_col]]
            .groupby(date_column, as_index=False)
            .sum()
        )
        grouped["failure_rate"] = (
            grouped[failure_count_col] / grouped[total_runs_col]
        ).fillna(0)
        grouped.rename(columns={date_column: "date"}, inplace=True)
        return grouped[["date", "failure_rate"]]

    fallback = (
        working[[date_column]]
        .assign(failure_rate=1.0)
        .groupby(date_column, as_index=False)
        .mean()
    )
    fallback.rename(columns={date_column: "date"}, inplace=True)
    return fallback


def top_failure_reasons(df: pd.DataFrame, limit: int = 10) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["failure_reason", "occurrences"])

    reason_column = "failure_reason" if "failure_reason" in df.columns else "root_cause_summary"
    count_column = "failure_count" if "failure_count" in df.columns else None

    if count_column and count_column in df.columns:
        grouped = (
            df.groupby(reason_column)[count_column]
            .sum()
            .reset_index(name="occurrences")
        )
    else:
        grouped = (
            df.groupby(reason_column)
            .size()
            .reset_index(name="occurrences")
        )
    grouped = grouped.sort_values("occurrences", ascending=False).head(limit)
    return grouped


def heatmap_matrix(df: pd.DataFrame, row: str, column: str, value: str = "failure_count") -> pd.DataFrame:
    if df.empty or row not in df.columns or column not in df.columns:
        return pd.DataFrame(columns=[row, column, "occurrences"])

    if value in df.columns:
        aggregated = df.groupby([row, column])[value].sum().reset_index()
        aggregated.rename(columns={value: "occurrences"}, inplace=True)
    else:
        aggregated = df.groupby([row, column]).size().reset_index(name="occurrences")
    return aggregated


def grouped_failure_table(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(
            columns=[
                "root_cause_summary",
                "impacted_tests",
                "owners",
                "last_occurrence",
                "log_links",
                "jira_links",
            ]
        )

    cause_column = "root_cause_summary" if "root_cause_summary" in df.columns else "root_cause"
    test_column = "test_name" if "test_name" in df.columns else "test_id"
    owner_column = "owner" if "owner" in df.columns else "owners"
    last_occurrence_column = (
        "last_occurrence" if "last_occurrence" in df.columns else "event_date"
    )

    def _row_from_group(group: pd.DataFrame) -> pd.Series:
        tests = ", ".join(sorted({str(v) for v in group[test_column].dropna()}))
        owners = ", ".join(sorted({str(v) for v in group[owner_column].dropna()})) if owner_column in group else ""
        last_seen = group[last_occurrence_column].max() if last_occurrence_column in group else pd.NaT
        logs = [url for url in group.get("log_url", pd.Series(dtype=str)).dropna().unique()]
        jira = [key for key in group.get("jira_ticket", pd.Series(dtype=str)).dropna().unique()]
        return pd.Series(
            {
                "impacted_tests": tests,
                "owners": owners,
                "last_occurrence": last_seen,
                "log_links": logs,
                "jira_links": jira,
            }
        )

    aggregated = (
        df.groupby(cause_column)
        .apply(_row_from_group)
        .reset_index()
        .rename(columns={cause_column: "root_cause_summary"})
    )

    return aggregated
