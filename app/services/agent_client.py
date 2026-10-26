"""Pass-through client for the external agent service.

Submits a query and subscribes once to that run's SSE stream. Events are
forwarded exactly as the agent sent them. This client does not generate run
status, insert delays, retry failed calls, or invoke MCP tools.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from typing import Any
from urllib.parse import quote

import httpx

from app.config import AgentSettings, load_config

SSE_TIMEOUT_SECONDS = 300
_TERMINAL_EVENTS = frozenset({"completed", "failed"})


class AgentTimeoutError(TimeoutError):
    """The agent did not respond within the configured timeout."""


class AgentConnectionError(ConnectionError):
    """The agent service could not be reached."""


class AgentHTTPError(Exception):
    """The agent returned an HTTP 4xx or 5xx response."""

    def __init__(self, status_code: int) -> None:
        self.status_code = status_code
        super().__init__(f"agent returned HTTP {status_code}")


class AgentClient:
    """HTTP client for query submission and a single SSE subscription."""

    def __init__(
        self,
        settings: AgentSettings | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._settings = load_config() if settings is None else settings
        self._transport = transport

    async def initiate_query(self, query_data: dict[str, Any]) -> dict[str, Any]:
        """POST the query and return the agent's immediate JSON reply.

        One request, no retries. The caller validates the payload and maps
        timeout, connection, and HTTP failures to a response status.
        """
        url = f"{self._base_url}/agent/query"
        try:
            async with self._open(self._settings.AGENT_TIMEOUT) as client:
                response = await client.post(url, json=query_data)
        except httpx.TimeoutException as exc:
            raise AgentTimeoutError("agent query timed out") from exc
        except httpx.RequestError as exc:
            raise AgentConnectionError("agent service unreachable") from exc
        if response.status_code >= 400:
            raise AgentHTTPError(response.status_code)
        return response.json()

    async def subscribe_to_run(self, agent_run_id: str) -> AsyncIterator[dict[str, Any]]:
        """Yield SSE events from one GET of the run stream.

        The connection closes after the first terminal ``completed`` or
        ``failed`` event. No further requests are made.

        If the agent disconnects before that terminal event — including a
        drop while the stream is still ``in_progress`` — this raises
        ``AgentConnectionError``. That failure is not retried and does not
        open another subscription (HLD Risk 3).
        """
        run_id = quote(agent_run_id, safe="")
        url = f"{self._base_url}/agent/runs/{run_id}/events"
        saw_terminal = False
        try:
            async with self._open(SSE_TIMEOUT_SECONDS) as client:
                async with client.stream(
                    "GET",
                    url,
                    headers={"Accept": "text/event-stream"},
                ) as response:
                    if response.status_code >= 400:
                        raise AgentHTTPError(response.status_code)
                    async for event in _parse_sse(response):
                        yield event
                        if event["event"] in _TERMINAL_EVENTS:
                            saw_terminal = True
                            break
            if not saw_terminal:
                raise AgentConnectionError("agent disconnected before a terminal event")
        except AgentHTTPError:
            raise
        except AgentConnectionError:
            raise
        except httpx.TimeoutException as exc:
            raise AgentTimeoutError("agent event stream timed out") from exc
        except httpx.RequestError as exc:
            raise AgentConnectionError("agent service unreachable") from exc

    @property
    def _base_url(self) -> str:
        return self._settings.AGENT_BASE_URL.rstrip("/")

    def _open(self, timeout_seconds: float) -> httpx.AsyncClient:
        kwargs: dict[str, Any] = {"timeout": httpx.Timeout(timeout_seconds)}
        if self._transport is not None:
            kwargs["transport"] = self._transport
        return httpx.AsyncClient(**kwargs)


async def _parse_sse(response: httpx.Response) -> AsyncIterator[dict[str, Any]]:
    """Parse ``event:`` / ``data:`` blocks. Does not invent missing events."""
    event_name: str | None = None
    data_lines: list[str] = []

    def dispatch() -> dict[str, Any] | None:
        nonlocal event_name, data_lines
        name = event_name
        lines = data_lines
        event_name = None
        data_lines = []
        if name is None or not lines:
            return None
        return {"event": name, "data": json.loads("\n".join(lines))}

    async for raw_line in response.aiter_lines():
        line = raw_line[:-1] if raw_line.endswith("\r") else raw_line
        if line == "":
            event = dispatch()
            if event is not None:
                yield event
            continue
        if line.startswith(":"):
            continue
        field, _, value = line.partition(":")
        if value.startswith(" "):
            value = value[1:]
        if field == "event":
            event_name = value
        elif field == "data":
            data_lines.append(value)

    trailing = dispatch()
    if trailing is not None:
        yield trailing
