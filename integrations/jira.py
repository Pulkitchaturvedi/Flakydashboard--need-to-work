"""Thin Jira client used by automation and API layers.

The implementation intentionally accepts callables for transport and time to
keep the code highly testable without pulling additional dependencies.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Callable, Dict, Optional


@dataclass
class JiraClient:
    """Minimal Jira client that works with any HTTP transport."""

    base_url: str
    project_key: str
    transport: Callable[[str, Dict[str, object]], Dict[str, object]]

    def create_ticket(self, summary: str, description: str) -> Dict[str, object]:
        payload = {
            "fields": {
                "project": {"key": self.project_key},
                "summary": summary,
                "description": description,
                "issuetype": {"name": "Bug"},
            }
        }
        return self.transport(f"{self.base_url}/rest/api/2/issue", payload)


@dataclass
class JiraTicketMetadata:
    """Metadata stored on unresolved root-cause groups."""

    key: str
    created_at: datetime
    url: Optional[str] = None
