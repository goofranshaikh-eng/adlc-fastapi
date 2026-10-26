"""POST /query immediate reply, registration, and agent failure mapping."""

import asyncio
import logging
import time
import uuid
from collections.abc import Iterator
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest
from fastapi.testclient import TestClient

from app.dependencies import get_agent_client
from app.main import app
from app.services.agent_client import (
    AgentClient,
    AgentConnectionError,
    AgentHTTPError,
    AgentTimeoutError,
)
from app.services.run_registry import RunRegistry, get_run_registry


def _assert_uuid4(value: str) -> None:
    parsed = uuid.UUID(value)
    assert parsed.version == 4
    assert str(parsed) == value
    assert len(value) == 36


@pytest.fixture
def agent_client() -> AsyncMock:
    return AsyncMock(spec=AgentClient)


@pytest.fixture
def registry() -> MagicMock:
    return MagicMock(spec=RunRegistry)


@pytest.fixture
def client(agent_client: AsyncMock, registry: MagicMock) -> Iterator[TestClient]:
    app.dependency_overrides[get_agent_client] = lambda: agent_client
    app.dependency_overrides[get_run_registry] = lambda: registry
    with TestClient(app) as test_client:
        yield test_client
    app.dependency_overrides.clear()


def test_get_agent_client_returns_the_same_instance() -> None:
    assert get_agent_client() is get_agent_client()
    assert isinstance(get_agent_client(), AgentClient)


def test_successful_query_returns_ok_immediately(
    client: TestClient,
    agent_client: AsyncMock,
    registry: MagicMock,
) -> None:
    agent_client.initiate_query.return_value = {
        "status": "ok",
        "agent_run_id": "agent-123",
    }

    started = time.perf_counter()
    response = client.post("/query", json={"query": "test query"})
    elapsed = time.perf_counter() - started

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ok"
    assert set(body) == {"status", "run_id"}
    _assert_uuid4(body["run_id"])
    assert elapsed < 0.1
    agent_client.initiate_query.assert_awaited_once_with({"query": "test query"})
    agent_client.subscribe_to_run.assert_not_called()
    registry.register.assert_called_once_with(
        run_id=body["run_id"],
        agent_run_id="agent-123",
    )


def test_concurrent_queries_return_within_100ms(
    agent_client: AsyncMock,
    registry: MagicMock,
) -> None:
    """AC1: 12 in-flight POSTs each return the agent reply in under 100ms."""
    agent_client.initiate_query.return_value = {
        "status": "ok",
        "agent_run_id": "agent-123",
    }
    app.dependency_overrides[get_agent_client] = lambda: agent_client
    app.dependency_overrides[get_run_registry] = lambda: registry

    async def scenario() -> tuple[list[tuple[httpx.Response, float]], float]:
        transport = httpx.ASGITransport(app=app)

        async def one(index: int) -> tuple[httpx.Response, float]:
            started = time.perf_counter()
            response = await http.post("/query", json={"query": f"test query {index}"})
            return response, time.perf_counter() - started

        async with httpx.AsyncClient(transport=transport, base_url="http://test") as http:
            started = time.perf_counter()
            results = await asyncio.gather(*(one(index) for index in range(12)))
            return list(results), time.perf_counter() - started

    try:
        results, elapsed = asyncio.run(scenario())
    finally:
        app.dependency_overrides.clear()

    assert elapsed < 0.1
    assert len(results) == 12
    run_ids = set()
    for response, request_elapsed in results:
        assert response.status_code == 200
        body = response.json()
        assert body["status"] == "ok"
        assert set(body) == {"status", "run_id"}
        _assert_uuid4(body["run_id"])
        run_ids.add(body["run_id"])
        assert request_elapsed < 0.1
    assert len(run_ids) == 12
    assert agent_client.initiate_query.await_count == 12
    agent_client.subscribe_to_run.assert_not_called()
    assert registry.register.call_count == 12


def test_rejected_query_returns_200_and_registers_run(
    client: TestClient,
    agent_client: AsyncMock,
    registry: MagicMock,
) -> None:
    agent_client.initiate_query.return_value = {
        "status": "rejected",
        "agent_run_id": "agent-456",
    }

    response = client.post("/query", json={"query": "bad query"})

    assert response.status_code == 200
    body = response.json()
    assert body == {"status": "rejected", "run_id": body["run_id"]}
    _assert_uuid4(body["run_id"])
    registry.register.assert_called_once_with(
        run_id=body["run_id"],
        agent_run_id="agent-456",
    )


@pytest.mark.parametrize(
    "error",
    [
        AgentTimeoutError("agent query timed out"),
        AgentConnectionError("agent service unreachable"),
        AgentHTTPError(500),
    ],
)
def test_agent_unavailable_returns_503_without_registering(
    client: TestClient,
    agent_client: AsyncMock,
    registry: MagicMock,
    caplog: pytest.LogCaptureFixture,
    error: Exception,
) -> None:
    agent_client.initiate_query.side_effect = error
    caplog.set_level(logging.ERROR)

    started = time.perf_counter()
    response = client.post("/query", json={"query": "test query"})
    elapsed = time.perf_counter() - started

    assert elapsed < 0.1
    assert response.status_code == 503
    assert response.json() == {"detail": "Agent service unavailable"}
    assert str(error) not in response.text
    assert str(error) in caplog.text
    registry.register.assert_not_called()
    agent_client.initiate_query.assert_awaited_once_with({"query": "test query"})


def test_registry_receives_generated_run_id_and_agent_run_id(
    client: TestClient,
    agent_client: AsyncMock,
    registry: MagicMock,
) -> None:
    agent_client.initiate_query.return_value = {
        "status": "ok",
        "agent_run_id": "agent-789",
    }

    response = client.post("/query", json={"query": "test query"})

    assert response.status_code == 200
    run_id = response.json()["run_id"]
    _assert_uuid4(run_id)
    assert run_id != "agent-789"
    registry.register.assert_called_once_with(
        run_id=run_id,
        agent_run_id="agent-789",
    )


def test_invalid_body_returns_422_without_calling_agent(
    client: TestClient,
    agent_client: AsyncMock,
    registry: MagicMock,
) -> None:
    response = client.post("/query", json={"query": ""})

    assert response.status_code == 422
    registry.register.assert_not_called()
    agent_client.initiate_query.assert_not_called()


def test_invalid_agent_reply_returns_500_without_registering(
    client: TestClient,
    agent_client: AsyncMock,
    registry: MagicMock,
) -> None:
    agent_client.initiate_query.return_value = {"status": "ok"}

    response = client.post("/query", json={"query": "test query"})

    assert response.status_code == 500
    assert response.json() == {"detail": "Internal server error"}
    registry.register.assert_not_called()


def test_unexpected_error_returns_500_without_registering(
    client: TestClient,
    agent_client: AsyncMock,
    registry: MagicMock,
    caplog: pytest.LogCaptureFixture,
) -> None:
    agent_client.initiate_query.side_effect = RuntimeError("database exploded")
    caplog.set_level(logging.ERROR)

    response = client.post("/query", json={"query": "test query"})

    assert response.status_code == 500
    assert response.json() == {"detail": "Internal server error"}
    assert "database exploded" not in response.text
    assert "database exploded" in caplog.text
    registry.register.assert_not_called()
