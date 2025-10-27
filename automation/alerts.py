"""Alerting helpers for flake anomalies.

The module provides reusable building blocks so the same alerting logic can be
wired into cron jobs, Airflow DAGs, or a CI pipeline.  Alerts are dispatched to
all registered notification channels (for example Slack and email).
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Callable, Iterable, List, Protocol, Sequence

from analytics.flake_metrics import WeeklyFlakeInsight, latest_anomalies


class Notifier(Protocol):
    """Abstract notification channel."""

    def send(self, subject: str, body: str) -> None:
        ...


@dataclass
class SlackNotifier:
    """Minimal Slack integration that posts JSON payloads to a webhook."""

    webhook: Callable[[str], None]
    channel: str

    def send(self, subject: str, body: str) -> None:  # pragma: no cover - thin wrapper
        payload = {
            "channel": self.channel,
            "username": "flake-monitor",
            "text": f"*{subject}*\n{body}",
        }
        self.webhook(json.dumps(payload))


@dataclass
class EmailNotifier:
    """Minimal email notifier that delegates to an injected transport."""

    transport: Callable[[dict], None]
    recipients: Sequence[str]

    def send(self, subject: str, body: str) -> None:  # pragma: no cover - thin wrapper
        self.transport({"to": list(self.recipients), "subject": subject, "body": body})


@dataclass(frozen=True)
class ThresholdConfig:
    """Thresholds for flake alerting."""

    max_flake_rate: float = 0.05
    max_wow_delta: float = 0.5
    max_z_score: float = 3.0


@dataclass
class AlertingEngine:
    """Evaluate insights and emit alerts when thresholds are breached."""

    notifiers: Sequence[Notifier]
    threshold: ThresholdConfig = field(default_factory=ThresholdConfig)

    def _notify(self, subject: str, body: str) -> None:
        for notifier in self.notifiers:
            notifier.send(subject, body)

    def run(self, insights: Iterable[WeeklyFlakeInsight]) -> None:
        insights = list(insights)
        if not insights:
            return

        latest = insights[-1]
        subject_bits: List[str] = []
        body_lines: List[str] = []

        if latest.flake_rate >= self.threshold.max_flake_rate:
            subject_bits.append("High flake rate")
            body_lines.append(
                f"Current week ({latest.week_start.isoformat()}) flake rate is {latest.flake_rate:.2%},"
                f" exceeding the threshold of {self.threshold.max_flake_rate:.2%}."
            )

        if latest.wow_delta is not None and latest.wow_delta >= self.threshold.max_wow_delta:
            subject_bits.append("Week-over-week spike")
            body_lines.append(
                f"Week-over-week delta is {latest.wow_delta:.2%} which is above the"
                f" allowed delta of {self.threshold.max_wow_delta:.2%}."
            )

        if latest.z_score is not None and latest.z_score >= self.threshold.max_z_score:
            subject_bits.append("Anomalous spike")
            body_lines.append(
                f"Latest Z-score is {latest.z_score:.2f}, beyond the threshold"
                f" of {self.threshold.max_z_score:.2f}."
            )

        anomalies = latest_anomalies(insights)
        if anomalies:
            formatted_rows: List[str] = []
            for item in anomalies:
                z_value = f"{item.z_score:.2f}" if item.z_score is not None else "N/A"
                formatted_rows.append(
                    f"- {item.week_start.isoformat()}: rate={item.flake_rate:.2%}, z={z_value}"
                )
            formatted = "\n".join(formatted_rows)
            body_lines.append("Recent anomalies:\n" + formatted)

        if subject_bits:
            subject = " | ".join(subject_bits)
            body = "\n".join(body_lines)
            self._notify(subject, body)
