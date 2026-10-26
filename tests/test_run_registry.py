"""Run registry ID mapping, collisions, lookup, and removal."""

import pytest

from app.services.run_registry import (
    RunIdCollisionError,
    RunNotFoundError,
    RunRegistry,
    get_run_registry,
)

RUN_ID = "123e4567-e89b-12d3-a456-426614174000"
AGENT_RUN_ID = "agent-run-123"
OTHER_RUN_ID = "123e4567-e89b-12d3-a456-426614174001"

_FORBIDDEN_METHODS = (
    "record_start",
    "record_completion",
    "record_completed",
    "record_failed",
    "get_elapsed_seconds",
    "get_status",
    "set_status",
    "start",
    "complete",
    "fail",
)

_FORBIDDEN_FIELDS = (
    "start_time",
    "completion_time",
    "completed_at",
    "status",
    "response",
    "error",
    "elapsed",
    "timestamp",
)


def test_register_stores_mapping_and_rejects_duplicate_run_id() -> None:
    registry = RunRegistry()

    registry.register(RUN_ID, AGENT_RUN_ID)

    assert registry.lookup(RUN_ID) == AGENT_RUN_ID

    with pytest.raises(RunIdCollisionError, match=f"run_id {RUN_ID} already exists"):
        registry.register(RUN_ID, "other-agent-run")

    assert registry.lookup(RUN_ID) == AGENT_RUN_ID


def test_lookup_returns_agent_run_id_and_raises_when_missing() -> None:
    registry = RunRegistry()
    registry.register(RUN_ID, AGENT_RUN_ID)

    assert registry.lookup(RUN_ID) == AGENT_RUN_ID

    with pytest.raises(RunNotFoundError, match=f"run_id {OTHER_RUN_ID} not found") as caught:
        registry.lookup(OTHER_RUN_ID)

    assert isinstance(caught.value, KeyError)
    assert OTHER_RUN_ID in str(caught.value)


def test_remove_deletes_mapping_and_raises_when_missing() -> None:
    registry = RunRegistry()
    registry.register(RUN_ID, AGENT_RUN_ID)

    registry.remove(RUN_ID)

    with pytest.raises(RunNotFoundError, match=f"run_id {RUN_ID} not found"):
        registry.lookup(RUN_ID)

    with pytest.raises(RunNotFoundError, match=f"run_id {OTHER_RUN_ID} not found"):
        registry.remove(OTHER_RUN_ID)


def test_lookup_of_unregistered_run_id_raises() -> None:
    registry = RunRegistry()

    with pytest.raises((KeyError, RunNotFoundError), match=RUN_ID) as caught:
        registry.lookup(RUN_ID)

    assert RUN_ID in str(caught.value)


def test_registry_stores_only_id_mapping() -> None:
    registry = RunRegistry()
    registry.register(RUN_ID, AGENT_RUN_ID)

    assert set(registry.__dict__) == {"_runs", "_lock"}
    assert registry._runs == {RUN_ID: AGENT_RUN_ID}
    assert all(isinstance(agent_run_id, str) for agent_run_id in registry._runs.values())

    for field in _FORBIDDEN_FIELDS:
        assert field not in registry.__dict__

    public_methods = {
        name
        for name in dir(registry)
        if callable(getattr(registry, name)) and not name.startswith("_")
    }
    assert public_methods == {"register", "lookup", "remove"}
    for name in _FORBIDDEN_METHODS:
        assert not hasattr(RunRegistry, name)


def test_get_run_registry_returns_the_same_instance() -> None:
    assert get_run_registry() is get_run_registry()
    assert isinstance(get_run_registry(), RunRegistry)
