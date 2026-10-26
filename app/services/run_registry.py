"""In-memory map from a client run_id to the agent's agent_run_id.

Used so the SSE endpoint can subscribe to the correct agent run. Stores only
that mapping. The agent owns status, timestamps, and any delay.
"""

from __future__ import annotations

import threading


class RunIdCollisionError(Exception):
    """Raised when a run_id is registered more than once."""


class RunNotFoundError(KeyError):
    """Raised when lookup or removal targets a run_id that is not mapped."""

    def __init__(self, run_id: str) -> None:
        self.run_id = run_id
        super().__init__(run_id)

    def __str__(self) -> str:
        return f"run_id {self.run_id} not found"


class RunRegistry:
    """Process-local run_id → agent_run_id map.

    Callers share one instance created at import. Story 4 injects it into
    endpoints. Cleanup of a mapping belongs in the SSE handler's finally block.
    """

    def __init__(self) -> None:
        self._runs: dict[str, str] = {}
        self._lock = threading.Lock()

    def register(self, run_id: str, agent_run_id: str) -> None:
        """Store run_id → agent_run_id. Reject a run_id that is already mapped."""
        with self._lock:
            if run_id in self._runs:
                raise RunIdCollisionError(f"run_id {run_id} already exists")
            self._runs[run_id] = agent_run_id

    def lookup(self, run_id: str) -> str:
        """Return the agent_run_id for run_id, or raise if it was never stored."""
        with self._lock:
            try:
                return self._runs[run_id]
            except KeyError:
                raise RunNotFoundError(run_id) from None

    def remove(self, run_id: str) -> None:
        """Drop the mapping after a terminal event. Raise if run_id is absent."""
        with self._lock:
            if run_id not in self._runs:
                raise RunNotFoundError(run_id)
            del self._runs[run_id]


_registry = RunRegistry()


def get_run_registry() -> RunRegistry:
    """Return the single registry created when this module is imported."""
    return _registry
