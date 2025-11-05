
# Flaky Test Analytics Dashboard

This repository provides a Streamlit dashboard that surfaces flaky test insights from the processed analytics tables in your data warehouse.

## Getting started

1. Create a virtual environment and install dependencies:
   ```bash
   pip install -r dashboard/requirements.txt
   ```
2. Configure the data source:
   - Set `ANALYTICS_DATABASE_URL` to a SQLAlchemy-compatible connection string that exposes the processed analytics tables (e.g., PostgreSQL, Snowflake, or BigQuery via `pybigquery`).
   - Optionally override the default table with `ANALYTICS_TABLE`. If you prefer to work offline, set `ANALYTICS_CSV_PATH` to a CSV export of the processed flaky test analytics.
3. Launch the dashboard:
   ```bash
   streamlit run dashboard/app.py
   ```

## Features

- Cascading sidebar filters for platform, team, pipeline, date range, and optional app version values.
- KPI cards for total flaky tests, unique root causes, and the latest failure-rate delta, alongside a time-series visualization.
- Visual summaries with bar charts for the leading failure reasons and heatmaps by team/platform and platform/pipeline intersections.
- A grouped failure table that highlights impacted tests, owners, last occurrence timestamps, and quick links to diagnostic logs or Jira tickets.
