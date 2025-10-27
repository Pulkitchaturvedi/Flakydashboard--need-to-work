"""Compute flaky test analytics and group related failures."""
from __future__ import annotations

import argparse
import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional, Sequence

try:  # pragma: no cover - dependency availability is environment specific
    import pandas as pd
except ModuleNotFoundError as error:  # pragma: no cover - handled lazily
    pd = None  # type: ignore[assignment]
    _PANDAS_IMPORT_ERROR = error
else:  # pragma: no cover - exercised when pandas is present
    _PANDAS_IMPORT_ERROR = None


@dataclass
class MetricsOutputPaths:
    """Container for materialized analytics outputs."""

    per_test_metrics: Path
    failure_groups: Path
    failure_group_runs: Path


ROOT_CAUSE_COMPONENTS: Sequence[str] = (
    "failure_reason",
    "stack_trace_hash",
    "error_code",
)

STABILITY_WINDOWS_DAYS: Dict[str, int] = {
    "stability_7d": 7,
    "stability_30d": 30,
}


def _require_pandas() -> None:
    if pd is None:
        message = (
            "pandas is required to compute flaky test analytics. Install pandas "
            "and retry."
        )
        raise ModuleNotFoundError(message) from _PANDAS_IMPORT_ERROR


def load_runs_dataframe(path: Path) -> pd.DataFrame:
    """Load test run metadata from the given file path.

    The loader automatically detects CSV, Parquet, and JSON files based on the
    file extension. The resulting DataFrame always contains a timezone-aware
    ``executed_at`` column converted to UTC.
    """

    _require_pandas()

    if not path.exists():
        raise FileNotFoundError(f"Run history file not found: {path}")

    if path.suffix == ".parquet":
        df = pd.read_parquet(path)
    elif path.suffix == ".csv":
        df = pd.read_csv(path)
    elif path.suffix in {".json", ".ndjson"}:
        df = pd.read_json(path, lines=path.suffix == ".ndjson")
    else:
        raise ValueError(
            "Unsupported input format. Expected .parquet, .csv, or .json files."
        )

    if "executed_at" not in df.columns:
        raise KeyError("Input data must include an 'executed_at' column")

    df = df.copy()
    df["executed_at"] = pd.to_datetime(df["executed_at"], utc=True)
    return df


def _normalize_component(value: object) -> str:
    if value is None:
        return ""
    if pd is not None and (isinstance(value, float) and pd.isna(value)):
        return ""
    return str(value).strip()


def assign_root_cause_group_ids(df: pd.DataFrame) -> pd.Series:
    """Assign a stable group identifier for each failed run.

    The identifier is derived from a deterministic hash of the concatenated
    failure metadata (reason, stack trace hash, error code). Empty components
    are skipped, ensuring semantically identical failures land in the same
    group even if some contextual metadata is missing.
    """

    _require_pandas()

    components = []
    for column in ROOT_CAUSE_COMPONENTS:
        if column not in df.columns:
            raise KeyError(
                f"Input data must include the '{column}' column for grouping"
            )
        components.append(df[column].map(_normalize_component))

    signature = pd.Series(["||".join(values) for values in zip(*components)], index=df.index)
    return signature.apply(lambda value: hashlib.sha1(value.encode()).hexdigest())


def compute_stability_windows(df: pd.DataFrame) -> pd.DataFrame:
    """Compute rolling stability metrics for each test case.

    The stability is defined as ``1 - failure_rate`` over the configured window
    length. A value of 1.0 represents perfect stability (no failures) while 0.0
    indicates the test failed in every run inside the window.
    """

    _require_pandas()

    if "executed_at" not in df.columns:
        raise KeyError("Data must include an 'executed_at' column for stability computation")
    if "test_name" not in df.columns:
        raise KeyError("Data must include a 'test_name' column for stability computation")
    if "failed" not in df.columns:
        raise KeyError("Data must include a boolean 'failed' column before computing stability")

    def _apply_windows(group: pd.DataFrame) -> pd.DataFrame:
        group = group.sort_values("executed_at").set_index("executed_at")
        for column, window_days in STABILITY_WINDOWS_DAYS.items():
            rolling = group["failed"].rolling(window=f"{window_days}D")
            counts = rolling.agg(["sum", "count"]).rename(
                columns={"sum": "failed_runs", "count": "total_runs"}
            )
            with pd.option_context("mode.use_inf_as_na", True):
                stability = 1.0 - (counts["failed_runs"] / counts["total_runs"])
            group[column] = stability.fillna(method="ffill").fillna(1.0)
        return group.reset_index()

    return df.groupby("test_name", group_keys=False).apply(_apply_windows)


def compute_per_test_flake_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Summarise flaky behaviour for each test."""

    _require_pandas()

    required_columns = {"test_name", "status", "executed_at"}
    missing = required_columns - set(df.columns)
    if missing:
        raise KeyError(f"Missing required columns for flake metrics: {sorted(missing)}")

    metrics_df = df.copy()
    status_normalized = metrics_df["status"].astype(str).str.lower()
    metrics_df["failed"] = status_normalized.isin({"failed", "error", "flake", "flaky"})

    stability_enriched = compute_stability_windows(metrics_df)
    totals = (
        stability_enriched.groupby("test_name")
        .agg(
            total_runs=("run_id" if "run_id" in stability_enriched.columns else "failed", "count"),
            failed_runs=("failed", "sum"),
        )
        .reset_index()
    )
    totals["flake_rate"] = totals["failed_runs"] / totals["total_runs"].where(totals["total_runs"] != 0, 1)

    latest_stability = (
        stability_enriched.sort_values("executed_at")
        .groupby("test_name")
        .tail(1)
        .set_index("test_name")
    )

    for column in STABILITY_WINDOWS_DAYS:
        totals[column] = latest_stability[column]

    if "platform" in stability_enriched.columns:
        latest_platform = (
            stability_enriched.sort_values("executed_at")
            .groupby("test_name")
            .tail(1)[["test_name", "platform"]]
        )
        totals = totals.merge(latest_platform, on="test_name", how="left")

    if "team" in stability_enriched.columns:
        latest_team = (
            stability_enriched.sort_values("executed_at")
            .groupby("test_name")
            .tail(1)[["test_name", "team"]]
        )
        totals = totals.merge(latest_team, on="test_name", how="left")

    return totals.sort_values("flake_rate", ascending=False)


def compute_failure_group_summaries(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate failure counts by platform, team, and root cause."""

    _require_pandas()

    required = {"failed", "platform", "team"}
    missing = required - set(df.columns)
    if missing:
        raise KeyError(
            f"Missing required columns for failure grouping: {sorted(missing)}"
        )

    failure_df = df[df["failed"]].copy()
    if failure_df.empty:
        return pd.DataFrame(
            columns=[
                "platform",
                "team",
                "root_cause_group_id",
                "failure_count",
                "affected_tests",
                "latest_failure_at",
            ]
        )

    failure_df["root_cause_group_id"] = assign_root_cause_group_ids(failure_df)

    group_counts = (
        failure_df.groupby(["platform", "team", "root_cause_group_id"])
        .agg(
            failure_count=("test_name", "count"),
            affected_tests=("test_name", lambda values: len(set(values))),
            latest_failure_at=("executed_at", "max"),
        )
        .reset_index()
    )

    return group_counts.sort_values("failure_count", ascending=False)


def build_failure_group_runs(df: pd.DataFrame) -> pd.DataFrame:
    """Materialise run-level drill-down data for each failure group."""

    _require_pandas()

    failure_df = df[df["failed"]].copy()
    if failure_df.empty:
        return pd.DataFrame(
            columns=[
                "root_cause_group_id",
                "test_name",
                "run_id",
                "executed_at",
                "run_url",
                "log_path",
                "failure_reason",
            ]
        )

    failure_df["root_cause_group_id"] = assign_root_cause_group_ids(failure_df)

    columns = [
        "root_cause_group_id",
        "test_name",
        "run_id" if "run_id" in failure_df.columns else None,
        "executed_at",
        "run_url" if "run_url" in failure_df.columns else None,
        "log_path" if "log_path" in failure_df.columns else None,
        "failure_reason",
    ]
    columns = [column for column in columns if column is not None and column in failure_df.columns]

    return failure_df[columns].sort_values("executed_at", ascending=False)


def save_metrics_outputs(
    per_test: pd.DataFrame,
    failure_groups: pd.DataFrame,
    group_runs: pd.DataFrame,
    output_paths: MetricsOutputPaths,
) -> None:
    """Persist computed analytics as parquet datasets."""

    _require_pandas()

    output_paths.per_test_metrics.parent.mkdir(parents=True, exist_ok=True)
    output_paths.failure_groups.parent.mkdir(parents=True, exist_ok=True)
    output_paths.failure_group_runs.parent.mkdir(parents=True, exist_ok=True)

    per_test.to_parquet(output_paths.per_test_metrics, index=False)
    failure_groups.to_parquet(output_paths.failure_groups, index=False)
    group_runs.to_parquet(output_paths.failure_group_runs, index=False)


def _default_output_paths(base_dir: Path) -> MetricsOutputPaths:
    return MetricsOutputPaths(
        per_test_metrics=base_dir / "per_test_flake_metrics.parquet",
        failure_groups=base_dir / "failure_groups.parquet",
        failure_group_runs=base_dir / "failure_group_runs.parquet",
    )


def run_pipeline(input_path: Path, output_dir: Path) -> MetricsOutputPaths:
    """Execute the full analytics pipeline for a run history dataset."""

    df = load_runs_dataframe(input_path)
    per_test_metrics = compute_per_test_flake_metrics(df)

    # reuse computed ``failed`` flag for grouping outputs
    df_with_failures = df.copy()
    df_with_failures["failed"] = df_with_failures["status"].astype(str).str.lower().isin(
        {"failed", "error", "flake", "flaky"}
    )
    failure_groups = compute_failure_group_summaries(df_with_failures)
    group_runs = build_failure_group_runs(df_with_failures)

    output_paths = _default_output_paths(output_dir)
    save_metrics_outputs(per_test_metrics, failure_groups, group_runs, output_paths)
    return output_paths


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Compute flaky test metrics and failure group analytics",
    )
    parser.add_argument(
        "input",
        type=Path,
        help="Path to the run history dataset (.parquet, .csv, or .json)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/processed"),
        help="Directory where parquet outputs should be written",
    )
    return parser


def main(argv: Optional[Sequence[str]] = None) -> MetricsOutputPaths:
    parser = build_argument_parser()
    args = parser.parse_args(argv)
    return run_pipeline(args.input, args.output_dir)


if __name__ == "__main__":
    main()
