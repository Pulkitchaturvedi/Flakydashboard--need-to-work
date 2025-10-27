"""Utilities for loading processed analytics data for the dashboard."""
from __future__ import annotations

import os
from functools import lru_cache
from typing import Optional

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

try:
    import streamlit as st
except ImportError:  # pragma: no cover - streamlit is only required for the app runtime
    st = None  # type: ignore


DEFAULT_TABLE = os.getenv("ANALYTICS_TABLE", "analytics.processed_flaky_tests")
DATE_COLUMNS = ("event_date", "test_date", "last_occurrence", "first_seen")


class MissingConfigurationError(RuntimeError):
    """Raised when the dashboard cannot locate a data source configuration."""


@lru_cache(maxsize=1)
def _get_cached_engine(database_url: str) -> Engine:
    """Create an engine for the given database URL and memoize the instance."""
    return create_engine(database_url)


def get_engine() -> Engine:
    """Return a SQLAlchemy engine based on the ``ANALYTICS_DATABASE_URL`` env var."""
    database_url = os.getenv("ANALYTICS_DATABASE_URL")
    if not database_url:
        raise MissingConfigurationError(
            "Set the ANALYTICS_DATABASE_URL environment variable or provide a "
            "fallback CSV via ANALYTICS_CSV_PATH."
        )
    return _get_cached_engine(database_url)


def _read_dataframe_from_source(query: Optional[str] = None) -> pd.DataFrame:
    """Read the analytics dataframe either from CSV or a SQL database."""
    csv_path = os.getenv("ANALYTICS_CSV_PATH")
    if csv_path and os.path.exists(csv_path):
        df = pd.read_csv(csv_path)
    else:
        engine = get_engine()
        sql = query or f"SELECT * FROM {DEFAULT_TABLE}"
        df = pd.read_sql(sql, engine)
    return df


def _ensure_datetime_columns(df: pd.DataFrame) -> pd.DataFrame:
    for column in DATE_COLUMNS:
        if column in df.columns:
            df[column] = pd.to_datetime(df[column], errors="coerce")
    return df


if st is not None:
    @st.cache_data(show_spinner=False, ttl=600)
    def load_flaky_test_data(query: Optional[str] = None) -> pd.DataFrame:
        """Load processed flaky test analytics with optional SQL override."""
        df = _read_dataframe_from_source(query)
        return _ensure_datetime_columns(df)
else:  # pragma: no cover - fallback when streamlit isn't available
    def load_flaky_test_data(query: Optional[str] = None) -> pd.DataFrame:
        df = _read_dataframe_from_source(query)
        return _ensure_datetime_columns(df)
