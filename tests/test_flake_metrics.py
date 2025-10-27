import pytest

pd = pytest.importorskip("pandas")

from analytics.flake_metrics import (
    assign_root_cause_group_ids,
    build_failure_group_runs,
    compute_failure_group_summaries,
    compute_per_test_flake_metrics,
)


def _sample_runs() -> pd.DataFrame:
    data = [
        {
            "run_id": "1",
            "test_name": "test_checkout",
            "status": "passed",
            "executed_at": "2024-01-01T00:00:00Z",
            "failure_reason": None,
            "stack_trace_hash": None,
            "error_code": None,
            "platform": "linux",
            "team": "checkout",
            "run_url": "https://ci.example/runs/1",
            "log_path": "/logs/1",
        },
        {
            "run_id": "2",
            "test_name": "test_checkout",
            "status": "failed",
            "executed_at": "2024-01-02T00:00:00Z",
            "failure_reason": "AssertionError: price mismatch",
            "stack_trace_hash": "abc123",
            "error_code": "E_ASSERT",
            "platform": "linux",
            "team": "checkout",
            "run_url": "https://ci.example/runs/2",
            "log_path": "/logs/2",
        },
        {
            "run_id": "3",
            "test_name": "test_checkout",
            "status": "passed",
            "executed_at": "2024-01-03T00:00:00Z",
            "failure_reason": None,
            "stack_trace_hash": None,
            "error_code": None,
            "platform": "linux",
            "team": "checkout",
            "run_url": "https://ci.example/runs/3",
            "log_path": "/logs/3",
        },
        {
            "run_id": "4",
            "test_name": "test_login",
            "status": "failed",
            "executed_at": "2024-01-01T12:00:00Z",
            "failure_reason": "TimeoutError",
            "stack_trace_hash": "def456",
            "error_code": "E_TIMEOUT",
            "platform": "mac",
            "team": "auth",
            "run_url": "https://ci.example/runs/4",
            "log_path": "/logs/4",
        },
        {
            "run_id": "5",
            "test_name": "test_login",
            "status": "failed",
            "executed_at": "2024-01-04T12:00:00Z",
            "failure_reason": "TimeoutError",
            "stack_trace_hash": "def456",
            "error_code": "E_TIMEOUT",
            "platform": "mac",
            "team": "auth",
            "run_url": "https://ci.example/runs/5",
            "log_path": "/logs/5",
        },
    ]
    df = pd.DataFrame(data)
    df["executed_at"] = pd.to_datetime(df["executed_at"], utc=True)
    return df


def test_compute_per_test_flake_metrics_returns_expected_rates():
    df = _sample_runs()
    metrics = compute_per_test_flake_metrics(df)
    metrics = metrics.set_index("test_name")

    checkout = metrics.loc["test_checkout"]
    assert checkout["total_runs"] == 3
    assert checkout["failed_runs"] == 1
    assert checkout["flake_rate"] == pytest.approx(1 / 3)
    assert checkout["stability_7d"] == pytest.approx(2 / 3)
    assert checkout["stability_30d"] == pytest.approx(2 / 3)

    login = metrics.loc["test_login"]
    assert login["total_runs"] == 2
    assert login["failed_runs"] == 2
    assert login["flake_rate"] == pytest.approx(1.0)
    assert login["stability_7d"] == pytest.approx(0.0)


def test_failure_grouping_clusters_same_root_cause():
    df = _sample_runs()
    df["failed"] = df["status"].isin(["failed", "error", "flake", "flaky"])

    failure_groups = compute_failure_group_summaries(df)
    assert {"linux", "mac"} == set(failure_groups["platform"])

    mac_group = failure_groups[failure_groups["platform"] == "mac"].iloc[0]
    assert mac_group["failure_count"] == 2
    assert mac_group["affected_tests"] == 1

    failure_df = df[df["failed"]]
    group_ids = assign_root_cause_group_ids(failure_df)
    mac_ids = group_ids[failure_df["platform"] == "mac"].unique()
    assert len(mac_ids) == 1


def test_build_failure_group_runs_includes_run_metadata():
    df = _sample_runs()
    df["failed"] = df["status"].isin(["failed", "error", "flake", "flaky"])

    drilldown = build_failure_group_runs(df)
    assert set(["test_name", "run_id", "run_url", "log_path"]).issubset(drilldown.columns)
    latest_run = drilldown.iloc[0]
    assert latest_run["run_id"] == "5"
    assert latest_run["run_url"] == "https://ci.example/runs/5"

