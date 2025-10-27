"""Streamlit dashboard for flaky test analytics."""
from __future__ import annotations

from datetime import date
from typing import Iterable, Optional

import pandas as pd
import plotly.express as px
import streamlit as st

from dashboard.analytics import (
    FilterSelections,
    apply_filters,
    compute_failure_rate_trend,
    compute_total_flaky_tests,
    compute_unique_root_causes,
    grouped_failure_table,
    heatmap_matrix,
    top_failure_reasons,
)
from dashboard.data_access import MissingConfigurationError, load_flaky_test_data


def _option_list(values: Iterable[Optional[str]]) -> list[str]:
    cleaned = sorted({str(v) for v in values if pd.notna(v) and str(v).strip()})
    return cleaned


def _date_bounds(df: pd.DataFrame) -> Optional[tuple[pd.Timestamp, pd.Timestamp]]:
    for column in ("event_date", "test_date"):
        if column in df.columns:
            series = df[column].dropna()
            if not series.empty:
                return series.min(), series.max()
    return None


def _format_links(urls: list[str], prefix: str) -> str:
    if not urls:
        return ""
    return ", ".join(f"[{prefix} {idx + 1}]({url})" for idx, url in enumerate(urls))


def render_kpis(df: pd.DataFrame) -> None:
    total_flaky = compute_total_flaky_tests(df)
    total_root_causes = compute_unique_root_causes(df)
    trend = compute_failure_rate_trend(df)

    last_rate = trend["failure_rate"].iloc[-1] if not trend.empty else 0
    previous_rate = trend["failure_rate"].iloc[-2] if len(trend) > 1 else None
    delta = None if previous_rate is None else last_rate - previous_rate

    col1, col2, col3 = st.columns(3)
    col1.metric("Total flaky tests", f"{total_flaky:,}")
    col2.metric("Unique root causes", f"{total_root_causes:,}")
    if delta is not None:
        col3.metric("Failure rate (latest)", f"{last_rate:.2%}", f"{delta:+.2%}")
    else:
        col3.metric("Failure rate (latest)", f"{last_rate:.2%}")

    if not trend.empty:
        st.plotly_chart(
            px.line(trend, x="date", y="failure_rate", title="Failure rate trend"),
            use_container_width=True,
        )


def render_top_failure_reasons(df: pd.DataFrame) -> None:
    top_reasons = top_failure_reasons(df)
    if top_reasons.empty:
        st.info("No failure reasons to display for the current filters.")
        return

    fig = px.bar(
        top_reasons,
        x="occurrences",
        y="failure_reason",
        orientation="h",
        title="Top failure reasons",
        labels={"occurrences": "Occurrences", "failure_reason": "Failure reason"},
    )
    fig.update_layout(yaxis=dict(autorange="reversed"))
    st.plotly_chart(fig, use_container_width=True)


def render_heatmaps(df: pd.DataFrame) -> None:
    heatmap_team_platform = heatmap_matrix(df, "team", "platform")
    heatmap_platform_pipeline = heatmap_matrix(df, "platform", "pipeline")

    cols = st.columns(2)

    if heatmap_team_platform.empty:
        cols[0].info("Insufficient data for team/platform heatmap.")
    else:
        fig = px.density_heatmap(
            heatmap_team_platform,
            x="platform",
            y="team",
            z="occurrences",
            color_continuous_scale="Blues",
            title="Failures by team and platform",
        )
        cols[0].plotly_chart(fig, use_container_width=True)

    if heatmap_platform_pipeline.empty:
        cols[1].info("Insufficient data for platform/pipeline heatmap.")
    else:
        fig = px.density_heatmap(
            heatmap_platform_pipeline,
            x="pipeline",
            y="platform",
            z="occurrences",
            color_continuous_scale="Viridis",
            title="Failures by platform and pipeline",
        )
        cols[1].plotly_chart(fig, use_container_width=True)


def render_grouped_failures(df: pd.DataFrame) -> None:
    grouped = grouped_failure_table(df)
    if grouped.empty:
        st.info("No grouped failures match the current filters.")
        return

    display = grouped.copy()
    display["Logs"] = display["log_links"].apply(lambda urls: _format_links(urls, "Log"))
    display["Jira"] = display["jira_links"].apply(lambda urls: _format_links(urls, "Jira"))
    display.drop(columns=["log_links", "jira_links"], inplace=True)

    st.dataframe(
        display,
        use_container_width=True,
        hide_index=True,
    )


def main() -> None:
    st.set_page_config(page_title="Flaky Test Analytics", layout="wide")
    st.title("Flaky Test Analytics Dashboard")
    st.caption("Monitor flaky test behavior and collaborate on remediation efforts.")

    try:
        raw_data = load_flaky_test_data()
    except MissingConfigurationError as exc:
        st.error(str(exc))
        st.stop()

    if raw_data.empty:
        st.warning("The analytics table returned no rows. Confirm the data pipeline is populated.")
        st.stop()

    with st.sidebar:
        st.header("Filters")

        platforms = _option_list(raw_data.get("platform", pd.Series(dtype=str)))
        selected_platforms = st.multiselect(
            "Platform",
            options=platforms,
            default=platforms,
        )
        platform_filtered = (
            raw_data[raw_data["platform"].isin(selected_platforms)]
            if selected_platforms
            else raw_data
        )

        teams = _option_list(platform_filtered.get("team", pd.Series(dtype=str)))
        selected_teams = st.multiselect(
            "Team",
            options=teams,
            default=teams,
        )
        team_filtered = (
            platform_filtered[platform_filtered["team"].isin(selected_teams)]
            if selected_teams
            else platform_filtered
        )

        pipelines = _option_list(team_filtered.get("pipeline", pd.Series(dtype=str)))
        selected_pipelines = st.multiselect(
            "Pipeline",
            options=pipelines,
            default=pipelines,
        )
        pipeline_filtered = (
            team_filtered[team_filtered["pipeline"].isin(selected_pipelines)]
            if selected_pipelines
            else team_filtered
        )

        app_versions = _option_list(pipeline_filtered.get("app_version", pd.Series(dtype=str)))
        selected_app_versions = st.multiselect(
            "App version (optional)",
            options=app_versions,
        )

        date_bounds = _date_bounds(pipeline_filtered)
        selected_dates: Optional[tuple[date, date]] = None
        if date_bounds:
            min_date, max_date = date_bounds
            default_range = (min_date.date(), max_date.date())
            selected_dates = st.date_input(
                "Date range",
                value=default_range,
                min_value=min_date.date(),
                max_value=max_date.date(),
            )
            if isinstance(selected_dates, date):
                selected_dates = (selected_dates, selected_dates)

    selections = FilterSelections(
        platforms=selected_platforms or None,
        teams=selected_teams or None,
        pipelines=selected_pipelines or None,
        app_versions=selected_app_versions or None,
        date_range=(
            pd.to_datetime(selected_dates[0]),
            pd.to_datetime(selected_dates[1]),
        )
        if selected_dates
        else None,
    )

    filtered = apply_filters(raw_data, selections)

    if filtered.empty:
        st.warning("No data matches the selected filters. Adjust the filters to see results.")
        st.stop()

    render_kpis(filtered)
    render_top_failure_reasons(filtered)
    render_heatmaps(filtered)

    st.subheader("Grouped failures")
    st.caption(
        "Root cause summaries grouped with impacted tests, owners, and helpful remediation links."
    )
    render_grouped_failures(filtered)


if __name__ == "__main__":
    main()
