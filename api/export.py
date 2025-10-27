"""Export endpoints for analytics data."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime, timedelta
from io import StringIO
from typing import Callable, Iterable, List, Optional, Sequence

from analytics.flake_metrics import WeeklyFlakeInsight
from integrations.jira import JiraClient, JiraTicketMetadata

try:  # pragma: no cover - optional dependency
    from fastapi import FastAPI, HTTPException
    from fastapi.responses import JSONResponse, PlainTextResponse
except ModuleNotFoundError:  # pragma: no cover - optional dependency
    FastAPI = None  # type: ignore

    class HTTPException(Exception):  # type: ignore
        def __init__(self, status_code: int, detail: str) -> None:
            super().__init__(detail)
            self.status_code = status_code
            self.detail = detail

    class JSONResponse(dict):  # type: ignore
        def __init__(self, content):  # pragma: no cover - fallback never used in production
            super().__init__({"content": content})

    class PlainTextResponse(str):  # type: ignore
        def __new__(cls, content, media_type: str = "text/plain"):  # pragma: no cover - fallback never used
            obj = str.__new__(cls, content)
            obj.media_type = media_type
            return obj


@dataclass
class RootCauseGroup:
    """Represents a flake root-cause group."""

    identifier: str
    summary: str
    created_at: datetime
    resolved_at: Optional[datetime] = None
    jira_ticket: Optional[JiraTicketMetadata] = None

    def is_unresolved(self) -> bool:
        return self.resolved_at is None


def generate_flake_csv(insights: Sequence[WeeklyFlakeInsight]) -> str:
    buffer = StringIO()
    writer = csv.writer(buffer)
    writer.writerow(
        [
            "week_start",
            "test_runs",
            "flake_failures",
            "flake_rate",
            "wow_delta",
            "z_score",
            "is_anomalous",
        ]
    )
    for item in insights:
        writer.writerow(
            [
                item.week_start.isoformat(),
                item.test_runs,
                item.flake_failures,
                round(item.flake_rate, 6),
                "" if item.wow_delta is None else round(item.wow_delta, 6),
                "" if item.z_score is None else round(item.z_score, 6),
                int(item.is_anomalous),
            ]
        )
    return buffer.getvalue()


def generate_flake_json(insights: Sequence[WeeklyFlakeInsight]) -> List[dict]:
    payload: List[dict] = []
    for item in insights:
        payload.append(
            {
                "week_start": item.week_start.isoformat(),
                "test_runs": item.test_runs,
                "flake_failures": item.flake_failures,
                "flake_rate": item.flake_rate,
                "wow_delta": item.wow_delta,
                "z_score": item.z_score,
                "is_anomalous": item.is_anomalous,
            }
        )
    return payload


def groups_exceeding_sla(
    groups: Iterable[RootCauseGroup],
    sla_days: int,
    now: Optional[datetime] = None,
) -> List[RootCauseGroup]:
    now = now or datetime.utcnow()
    overdue: List[RootCauseGroup] = []
    deadline = timedelta(days=sla_days)
    for group in groups:
        if group.is_unresolved() and now - group.created_at >= deadline:
            overdue.append(group)
    return overdue


def ensure_jira_tickets(
    groups: Iterable[RootCauseGroup],
    sla_days: int,
    jira_client: JiraClient,
    now: Optional[datetime] = None,
) -> List[JiraTicketMetadata]:
    tickets: List[JiraTicketMetadata] = []
    for group in groups_exceeding_sla(groups, sla_days, now=now):
        if group.jira_ticket is not None:
            tickets.append(group.jira_ticket)
            continue
        description = (
            "Automated escalation triggered because the root-cause group "
            f"'{group.summary}' has been unresolved since {group.created_at.isoformat()}"
        )
        response = jira_client.create_ticket(
            summary=f"Flake group {group.identifier} unresolved",
            description=description,
        )
        key = response.get("key", "UNKNOWN")
        ticket = JiraTicketMetadata(key=key, created_at=datetime.utcnow(), url=response.get("self"))
        group.jira_ticket = ticket
        tickets.append(ticket)
    return tickets


def create_app(
    insights_provider: Callable[[], Sequence[WeeklyFlakeInsight]],
    group_provider: Callable[[], Iterable[RootCauseGroup]],
    jira_client: Optional[JiraClient] = None,
    sla_days: int = 5,
):  # pragma: no cover - requires FastAPI
    if FastAPI is None:
        raise RuntimeError("FastAPI is required to build the export API. Install fastapi to proceed.")

    app = FastAPI(title="Flake analytics exports")

    @app.get("/exports/flake-metrics.csv")
    def flake_metrics_csv():
        insights = insights_provider()
        csv_payload = generate_flake_csv(insights)
        return PlainTextResponse(csv_payload, media_type="text/csv")

    @app.get("/exports/flake-metrics.json")
    def flake_metrics_json():
        insights = insights_provider()
        return JSONResponse(generate_flake_json(insights))

    @app.post("/integrations/jira/ensure")
    def ensure_jira():
        if jira_client is None:
            raise HTTPException(status_code=400, detail="Jira integration not configured")
        groups = list(group_provider())
        tickets = ensure_jira_tickets(groups, sla_days, jira_client)
        return {"tickets": [ticket.key for ticket in tickets]}

    return app
