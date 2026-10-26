"""Agent client query submission and single SSE subscription."""

import ast
import asyncio
import json
import time
from collections.abc import Callable
from pathlib import Path

import httpx

from app.config import AgentSettings
from app.services import agent_client as agent_client_module
from app.services.agent_client import (
    SSE_TIMEOUT_SECONDS,
    AgentClient,
    AgentConnectionError,
    AgentHTTPError,
    AgentTimeoutError,
)

SETTINGS = AgentSettings(
    AGENT_BASE_URL="http://agent.test",
    AGENT_TIMEOUT=3,
)

SUCCESS_STREAM = """event: in_progress
data: {"run_id": "run-123"}

event: completed
data: {"run_id": "run-123", "response": {"result": "success"}}

"""

FAILURE_STREAM = """event: in_progress
data: {"run_id": "run-123"}

event: failed
data: {"run_id": "run-123", "error": "agent timeout"}

"""

STREAM_WITH_TRAILING_EVENT = """event: in_progress
data: {"run_id": "run-123"}

event: completed
data: {"run_id": "run-123", "response": {"result": "success"}}

event: in_progress
data: {"run_id": "run-123"}

"""

IN_PROGRESS_ONLY = """event: in_progress
data: {"run_id": "run-123"}

"""


class TrackingTransport(httpx.MockTransport):
    """Mock transport that records requests and whether the client closed it."""

    def __init__(self, handler: Callable[[httpx.Request], httpx.Response]) -> None:
        super().__init__(handler)
        self.requests: list[httpx.Request] = []
        self.closed = False

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        return await super().handle_async_request(request)

    async def aclose(self) -> None:
        self.closed = True
        await super().aclose()


def _transport(handler) -> TrackingTransport:
    return TrackingTransport(handler)


def test_successful_query_submission() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={"status": "ok", "agent_run_id": "run-123"},
        )

    transport = _transport(handler)

    async def scenario() -> None:
        client = AgentClient(settings=SETTINGS, transport=transport)
        started = time.perf_counter()
        result = await client.initiate_query({"query": "test query"})
        elapsed = time.perf_counter() - started
        assert result == {"status": "ok", "agent_run_id": "run-123"}
        assert elapsed < 0.1

    asyncio.run(scenario())
    assert len(transport.requests) == 1
    request = transport.requests[0]
    assert request.method == "POST"
    assert request.url.path == "/agent/query"
    assert json.loads(request.content) == {"query": "test query"}


def test_rejected_query_submission() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={"status": "rejected", "reason": "invalid query"},
        )

    transport = _transport(handler)

    async def scenario() -> None:
        client = AgentClient(settings=SETTINGS, transport=transport)
        result = await client.initiate_query({"query": "bad"})
        assert result["status"] == "rejected"
        assert result["reason"] == "invalid query"

    asyncio.run(scenario())
    assert len(transport.requests) == 1
    assert transport.requests[0].method == "POST"


def test_query_submission_timeout(monkeypatch) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.TimeoutException("timed out", request=request)

    transport = _transport(handler)
    opened: list[httpx.Timeout] = []
    real_client = httpx.AsyncClient

    def capturing_client(*args, **kwargs):
        opened.append(kwargs["timeout"])
        return real_client(*args, **kwargs)

    monkeypatch.setattr(agent_client_module.httpx, "AsyncClient", capturing_client)

    async def scenario() -> None:
        client = AgentClient(settings=SETTINGS, transport=transport)
        status_code = None
        try:
            await client.initiate_query({"query": "slow"})
        except AgentTimeoutError:
            status_code = 503
        assert status_code == 503

    asyncio.run(scenario())
    assert len(transport.requests) == 1
    assert opened[0].read == SETTINGS.AGENT_TIMEOUT


def test_query_submission_connection_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused", request=request)

    transport = _transport(handler)

    async def scenario() -> None:
        client = AgentClient(settings=SETTINGS, transport=transport)
        try:
            await client.initiate_query({"query": "offline"})
        except AgentConnectionError:
            return
        raise AssertionError("expected AgentConnectionError")

    asyncio.run(scenario())
    assert len(transport.requests) == 1


def test_query_submission_http_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(502, json={"error": "bad gateway"})

    transport = _transport(handler)

    async def scenario() -> None:
        client = AgentClient(settings=SETTINGS, transport=transport)
        try:
            await client.initiate_query({"query": "boom"})
        except AgentHTTPError as exc:
            assert exc.status_code == 502
            return
        raise AssertionError("expected AgentHTTPError")

    asyncio.run(scenario())
    assert len(transport.requests) == 1


def test_client_reads_base_url_from_config(monkeypatch) -> None:
    monkeypatch.setenv("AGENT_BASE_URL", "http://configured.test:9000")
    monkeypatch.setenv("AGENT_TIMEOUT", "3")

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"status": "ok", "agent_run_id": "run-1"})

    transport = _transport(handler)

    async def scenario() -> dict:
        client = AgentClient(transport=transport)
        return await client.initiate_query({"query": "from config"})

    result = asyncio.run(scenario())
    assert result == {"status": "ok", "agent_run_id": "run-1"}
    assert str(transport.requests[0].url) == "http://configured.test:9000/agent/query"


def test_sse_stream_success_path(monkeypatch) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text=SUCCESS_STREAM)

    transport = _transport(handler)
    opened: list[httpx.Timeout] = []
    real_client = httpx.AsyncClient

    def capturing_client(*args, **kwargs):
        opened.append(kwargs["timeout"])
        return real_client(*args, **kwargs)

    monkeypatch.setattr(agent_client_module.httpx, "AsyncClient", capturing_client)

    async def scenario() -> list[dict]:
        client = AgentClient(settings=SETTINGS, transport=transport)
        return [event async for event in client.subscribe_to_run("run-123")]

    events = asyncio.run(scenario())
    assert events == [
        {"event": "in_progress", "data": {"run_id": "run-123"}},
        {
            "event": "completed",
            "data": {"run_id": "run-123", "response": {"result": "success"}},
        },
    ]
    assert len(transport.requests) == 1
    request = transport.requests[0]
    assert request.method == "GET"
    assert request.url.path == "/agent/runs/run-123/events"
    assert request.headers["accept"] == "text/event-stream"
    assert opened[0].read == SSE_TIMEOUT_SECONDS


def test_sse_stream_failure_event() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text=FAILURE_STREAM)

    transport = _transport(handler)

    async def scenario() -> list[dict]:
        client = AgentClient(settings=SETTINGS, transport=transport)
        return [event async for event in client.subscribe_to_run("run-123")]

    events = asyncio.run(scenario())
    assert len(events) == 2
    assert events[0] == {"event": "in_progress", "data": {"run_id": "run-123"}}
    assert events[1]["event"] == "failed"
    assert events[1]["data"] == {"run_id": "run-123", "error": "agent timeout"}
    assert len(transport.requests) == 1
    assert transport.requests[0].method == "GET"


def test_single_connection_per_subscription() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text=STREAM_WITH_TRAILING_EVENT)

    transport = _transport(handler)

    async def scenario() -> list[dict]:
        client = AgentClient(settings=SETTINGS, transport=transport)
        return [event async for event in client.subscribe_to_run("run-123")]

    events = asyncio.run(scenario())
    assert [event["event"] for event in events] == ["in_progress", "completed"]
    assert len(transport.requests) == 1
    assert transport.closed is True


def test_disconnect_during_in_progress_is_error_without_retry() -> None:
    """A drop before a terminal event is an error and opens no second request.

    HLD Risk 3: disconnection while the stream is still ``in_progress`` is
    not a silent end and is not retried.
    """

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text=IN_PROGRESS_ONLY)

    transport = _transport(handler)

    async def scenario() -> list[dict]:
        client = AgentClient(settings=SETTINGS, transport=transport)
        events: list[dict] = []
        try:
            async for event in client.subscribe_to_run("run-123"):
                events.append(event)
        except AgentConnectionError:
            return events
        raise AssertionError("expected AgentConnectionError")

    events = asyncio.run(scenario())
    assert events == [{"event": "in_progress", "data": {"run_id": "run-123"}}]
    assert len(transport.requests) == 1
    assert transport.requests[0].method == "GET"
    assert transport.closed is True


def test_agent_client_has_no_mcp_imports() -> None:
    root = Path(__file__).resolve().parents[1] / "app" / "services"
    for path in root.glob("*.py"):
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                modules = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom):
                modules = [node.module or ""]
            else:
                continue
            assert all("mcp" not in module.lower() for module in modules)
