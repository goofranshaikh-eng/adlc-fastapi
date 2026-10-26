"""GET /query/{run_id}/stream forwarding, terminal close, and cleanup."""

import asyncio
import json
import time
from collections.abc import AsyncIterator, Iterator
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest
from fastapi.testclient import TestClient

from app.config import AgentSettings
from app.dependencies import get_agent_client
from app.main import app
from app.services.agent_client import (
    AgentClient,
    AgentConnectionError,
    AgentHTTPError,
    AgentTimeoutError,
)
from app.services.run_registry import RunNotFoundError, RunRegistry, get_run_registry

RUN_ID = "123e4567-e89b-12d3-a456-426614174000"
AGENT_RUN_ID = "agent-123"

IN_PROGRESS = {"event": "in_progress", "data": {"run_id": RUN_ID}}
COMPLETED = {
    "event": "completed",
    "data": {"run_id": RUN_ID, "response": {"result": "success"}},
}
FAILED = {"event": "failed", "data": {"run_id": RUN_ID, "error": "agent timeout"}}
UNEXPECTED = {"event": "in_progress", "data": {"run_id": RUN_ID, "extra": True}}


def parse_sse(payload: str) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for block in payload.split("\n\n"):
        if not block.strip():
            continue
        event_type = None
        data = None
        for line in block.split("\n"):
            if line.startswith("event:"):
                event_type = line.split(":", 1)[1].strip()
            elif line.startswith("data:"):
                data = json.loads(line.split(":", 1)[1].strip())
        events.append({"event": event_type, "data": data})
    return events


class ScriptedAgent:
    """One subscription that yields scripted events, then an optional error."""

    def __init__(
        self,
        events: list[dict[str, Any]],
        error: Exception | None = None,
        hold: asyncio.Event | None = None,
    ) -> None:
        self.events = events
        self.error = error
        self.hold = hold
        self.subscribed: list[str] = []
        self.produced: list[str] = []
        self.closed = False

    async def subscribe_to_run(self, agent_run_id: str) -> AsyncIterator[dict[str, Any]]:
        self.subscribed.append(agent_run_id)
        try:
            for event in self.events:
                self.produced.append(event["event"])
                yield event
            if self.error is not None:
                raise self.error
            if self.hold is not None:
                await self.hold.wait()
        finally:
            self.closed = True


@pytest.fixture
def registry() -> MagicMock:
    registry = MagicMock(spec=RunRegistry)
    registry.lookup.return_value = AGENT_RUN_ID
    return registry


@pytest.fixture
def client(registry: MagicMock) -> Iterator[tuple[TestClient, ScriptedAgent]]:
    agent = ScriptedAgent([])
    app.dependency_overrides[get_agent_client] = lambda: agent
    app.dependency_overrides[get_run_registry] = lambda: registry
    with TestClient(app) as test_client:
        yield test_client, agent
    app.dependency_overrides.clear()


def _open(
    test_client: TestClient,
    agent: ScriptedAgent,
    events: list[dict[str, Any]],
    error: Exception | None = None,
) -> tuple[Any, float]:
    agent.events = events
    agent.error = error
    started = time.perf_counter()
    response = test_client.get(f"/query/{RUN_ID}/stream")
    elapsed = time.perf_counter() - started
    return response, elapsed


def test_stream_forwards_agent_events(
    client: tuple[TestClient, ScriptedAgent],
    registry: MagicMock,
) -> None:
    test_client, agent = client
    response, elapsed = _open(test_client, agent, [IN_PROGRESS, COMPLETED])

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/event-stream")
    assert parse_sse(response.text) == [IN_PROGRESS, COMPLETED]
    assert response.text.startswith("event: in_progress\ndata: ")
    assert elapsed < 1
    assert agent.subscribed == [AGENT_RUN_ID]
    registry.lookup.assert_called_once_with(RUN_ID)
    registry.remove.assert_called_once_with(RUN_ID)


def test_stream_forwards_failure_event(
    client: tuple[TestClient, ScriptedAgent],
    registry: MagicMock,
) -> None:
    test_client, agent = client
    response, _elapsed = _open(test_client, agent, [IN_PROGRESS, FAILED])

    events = parse_sse(response.text)
    assert [event["event"] for event in events] == ["in_progress", "failed"]
    assert events == [IN_PROGRESS, FAILED]
    assert all(event["event"] != "completed" for event in events)
    registry.remove.assert_called_once_with(RUN_ID)


def test_stream_closes_after_completed_event(
    client: tuple[TestClient, ScriptedAgent],
    registry: MagicMock,
) -> None:
    test_client, agent = client
    response, _elapsed = _open(
        test_client,
        agent,
        [IN_PROGRESS, COMPLETED, UNEXPECTED],
    )

    assert parse_sse(response.text) == [IN_PROGRESS, COMPLETED]
    assert agent.produced == ["in_progress", "completed"]
    registry.remove.assert_called_once_with(RUN_ID)


def test_stream_closes_after_failed_event(
    client: tuple[TestClient, ScriptedAgent],
    registry: MagicMock,
) -> None:
    test_client, agent = client
    response, _elapsed = _open(test_client, agent, [IN_PROGRESS, FAILED, UNEXPECTED])

    assert parse_sse(response.text) == [IN_PROGRESS, FAILED]
    assert agent.produced == ["in_progress", "failed"]
    registry.remove.assert_called_once_with(RUN_ID)


def test_stream_stops_after_first_terminal_event_even_if_agent_sends_more(
    client: tuple[TestClient, ScriptedAgent],
    registry: MagicMock,
) -> None:
    """AD-4: the first terminal event closes the stream, so both are never sent."""
    test_client, agent = client
    cases = (
        (
            [IN_PROGRESS, COMPLETED, FAILED, UNEXPECTED],
            [IN_PROGRESS, COMPLETED],
            ["in_progress", "completed"],
        ),
        (
            [IN_PROGRESS, FAILED, COMPLETED, UNEXPECTED],
            [IN_PROGRESS, FAILED],
            ["in_progress", "failed"],
        ),
    )
    for events, expected, produced in cases:
        agent.produced.clear()
        agent.subscribed.clear()
        registry.remove.reset_mock()
        response, _elapsed = _open(test_client, agent, events)

        parsed = parse_sse(response.text)
        assert parsed == expected
        terminals = [
            event["event"] for event in parsed if event["event"] in {"completed", "failed"}
        ]
        assert terminals == [expected[-1]["event"]]
        assert agent.produced == produced
        assert agent.subscribed == [AGENT_RUN_ID]
        registry.remove.assert_called_once_with(RUN_ID)


def test_missing_run_returns_404_without_subscribing(
    client: tuple[TestClient, ScriptedAgent],
    registry: MagicMock,
) -> None:
    test_client, agent = client
    registry.lookup.side_effect = RunNotFoundError(RUN_ID)

    response = test_client.get(f"/query/{RUN_ID}/stream")

    assert response.status_code == 404
    assert response.json() == {"detail": "Run not found"}
    assert agent.subscribed == []
    registry.remove.assert_not_called()


def test_subscription_timeout_sends_failed_event(
    client: tuple[TestClient, ScriptedAgent],
    registry: MagicMock,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Timeout emits a backend-generated ``failed`` event, not an agent event.

    AC3 still holds on the normal path: agent status is forwarded and not
    invented. When the subscription times out, this process itself creates
    ``failed`` with "Agent timeout". That exception is the HLD Risk 3
    mitigation, distinct from a ``failed`` event the agent already sent.
    """
    test_client, agent = client
    caplog.set_level("ERROR")
    response, elapsed = _open(
        test_client,
        agent,
        [IN_PROGRESS],
        error=AgentTimeoutError("agent event stream timed out"),
    )

    assert elapsed < 1
    assert parse_sse(response.text) == [
        IN_PROGRESS,
        {"event": "failed", "data": {"run_id": RUN_ID, "error": "Agent timeout"}},
    ]
    assert "agent event stream timed out" not in response.text
    assert "agent event stream timed out" in caplog.text
    registry.remove.assert_called_once_with(RUN_ID)


@pytest.mark.parametrize(
    "error",
    [AgentHTTPError(502), AgentConnectionError("agent service unreachable")],
)
def test_subscription_error_sends_generic_failed_event(
    client: tuple[TestClient, ScriptedAgent],
    registry: MagicMock,
    caplog: pytest.LogCaptureFixture,
    error: Exception,
) -> None:
    """Connection and HTTP failures are one backend `failed` event, not a retry.

    The agent already sent ``in_progress``. The ``failed`` frame is created
    here as exception recovery. It is not an event the agent yielded, and
    the endpoint does not open a second subscription.
    """
    test_client, agent = client
    caplog.set_level("ERROR")
    response, _elapsed = _open(test_client, agent, [IN_PROGRESS], error=error)

    assert parse_sse(response.text) == [
        IN_PROGRESS,
        {"event": "failed", "data": {"run_id": RUN_ID, "error": "Stream error"}},
    ]
    assert str(error) not in response.text
    assert str(error) in caplog.text
    assert agent.subscribed == [AGENT_RUN_ID]
    assert agent.closed is True
    registry.remove.assert_called_once_with(RUN_ID)


def test_agent_disconnect_during_in_progress_sends_stream_error_without_retry(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A stream that ends during in_progress is an error, not a silent retry.

    HLD Risk 3: the agent connection closes before a terminal event. The
    endpoint emits one backend-generated ``failed`` event ("Stream error")
    and the client makes no further HTTP call.
    """
    caplog.set_level("ERROR")
    requests: list[httpx.Request] = []
    body = (
        "event: in_progress\n"
        f'data: {{"run_id": "{RUN_ID}"}}\n'
        "\n"
    )

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, text=body)

    agent = AgentClient(
        settings=AgentSettings(AGENT_BASE_URL="http://agent.test", AGENT_TIMEOUT=3),
        transport=httpx.MockTransport(handler),
    )
    registry = RunRegistry()
    registry.register(run_id=RUN_ID, agent_run_id=AGENT_RUN_ID)
    app.dependency_overrides[get_agent_client] = lambda: agent
    app.dependency_overrides[get_run_registry] = lambda: registry
    try:
        with TestClient(app) as test_client:
            response = test_client.get(f"/query/{RUN_ID}/stream")
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert parse_sse(response.text) == [
        IN_PROGRESS,
        {"event": "failed", "data": {"run_id": RUN_ID, "error": "Stream error"}},
    ]
    assert "agent disconnected before a terminal event" not in response.text
    assert "agent disconnected before a terminal event" in caplog.text
    assert len(requests) == 1
    assert requests[0].method == "GET"
    assert requests[0].url.path == f"/agent/runs/{AGENT_RUN_ID}/events"
    with pytest.raises(RunNotFoundError):
        registry.lookup(RUN_ID)


def test_client_disconnection_triggers_cleanup(registry: MagicMock) -> None:
    """Disconnect cancels the local subscription and drops the registry entry.

    The agent run is not cancelled; its background job continues.
    """
    asyncio.run(_assert_disconnect_cleans_up(registry))


async def _assert_disconnect_cleans_up(registry: MagicMock) -> None:
    hold = asyncio.Event()
    agent = ScriptedAgent([IN_PROGRESS], hold=hold)
    app.dependency_overrides[get_agent_client] = lambda: agent
    app.dependency_overrides[get_run_registry] = lambda: registry
    try:
        status_code = await asyncio.wait_for(_drive_until_disconnect(), timeout=2)
    finally:
        app.dependency_overrides.clear()

    assert status_code == 200
    assert agent.produced == ["in_progress"]
    assert agent.closed is True
    registry.remove.assert_called_once_with(RUN_ID)
    assert not hold.is_set()


async def _drive_until_disconnect() -> int:
    request_sent = False
    disconnect = asyncio.Event()
    status_code = 0

    scope = {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.3"},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "http",
        "path": f"/query/{RUN_ID}/stream",
        "raw_path": f"/query/{RUN_ID}/stream".encode(),
        "query_string": b"",
        "headers": [(b"accept", b"text/event-stream")],
        "client": ("127.0.0.1", 123),
        "server": ("testserver", 80),
        "root_path": "",
    }

    async def receive() -> dict[str, Any]:
        nonlocal request_sent
        if not request_sent:
            request_sent = True
            return {"type": "http.request", "body": b"", "more_body": False}
        await disconnect.wait()
        return {"type": "http.disconnect"}

    async def send(message: dict[str, Any]) -> None:
        nonlocal status_code
        if message["type"] == "http.response.start":
            status_code = message["status"]
            return
        body = message.get("body") or b""
        if body:
            disconnect.set()

    await app(scope, receive, send)
    return status_code


def test_multiple_runs_cleaned_up_after_terminal_events() -> None:
    registry = RunRegistry()

    async def initiate_query(query_data: dict[str, Any]) -> dict[str, str]:
        return {"status": "ok", "agent_run_id": f"agent-{query_data['query']}"}

    async def subscribe_to_run(agent_run_id: str) -> AsyncIterator[dict[str, Any]]:
        yield {"event": "in_progress", "data": {"run_id": agent_run_id}}
        yield {
            "event": "completed",
            "data": {"run_id": agent_run_id, "response": {"result": "success"}},
        }

    agent = AsyncMock(spec=AgentClient)
    agent.initiate_query = initiate_query
    agent.subscribe_to_run = subscribe_to_run
    app.dependency_overrides[get_agent_client] = lambda: agent
    app.dependency_overrides[get_run_registry] = lambda: registry
    try:
        with TestClient(app) as test_client:
            run_ids = []
            for index in range(10):
                response = test_client.post("/query", json={"query": f"q{index}"})
                assert response.status_code == 200
                run_ids.append(response.json()["run_id"])

            assert len(registry._runs) == 10
            for run_id in run_ids:
                response = test_client.get(f"/query/{run_id}/stream")
                assert response.status_code == 200
                assert [event["event"] for event in parse_sse(response.text)] == [
                    "in_progress",
                    "completed",
                ]
    finally:
        app.dependency_overrides.clear()

    assert registry._runs == {}
    for run_id in run_ids:
        with pytest.raises(RunNotFoundError):
            registry.lookup(run_id)
