"""Process-wide service instances for endpoint injection.

`AgentClient` and `RunRegistry` are created once when the application loads.
Tests replace these callables through FastAPI dependency overrides.
"""

from app.services.agent_client import AgentClient
from app.services.run_registry import RunRegistry, get_run_registry

_agent_client = AgentClient()


def get_agent_client() -> AgentClient:
    """Return the agent client created when this module is imported."""
    return _agent_client


__all__ = [
    "AgentClient",
    "RunRegistry",
    "get_agent_client",
    "get_run_registry",
]
