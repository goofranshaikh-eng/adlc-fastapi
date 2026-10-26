"""Agent service settings loaded from the environment."""

from __future__ import annotations

import os
from dataclasses import dataclass

DEFAULT_AGENT_BASE_URL = "http://localhost:8001"
DEFAULT_AGENT_TIMEOUT = 3

_SETTINGS_FIELDS = ("AGENT_BASE_URL", "AGENT_TIMEOUT")


@dataclass(frozen=True)
class AgentSettings:
    """Connection parameters for the agent service."""

    AGENT_BASE_URL: str
    AGENT_TIMEOUT: int


def load_config(environ: dict[str, str] | None = None) -> AgentSettings:
    """Load agent settings, applying defaults when variables are absent.

    `AGENT_TIMEOUT` is parsed as an integer. Attribute access on this module
    (`config.AGENT_BASE_URL`, `config.AGENT_TIMEOUT`) reads the process
    environment on each lookup.
    """
    source = os.environ if environ is None else environ
    base_url = source.get("AGENT_BASE_URL", DEFAULT_AGENT_BASE_URL)
    timeout_raw = source.get("AGENT_TIMEOUT", str(DEFAULT_AGENT_TIMEOUT))
    try:
        timeout = int(timeout_raw)
    except (TypeError, ValueError) as exc:
        raise ValueError("AGENT_TIMEOUT must be an integer") from exc
    return AgentSettings(AGENT_BASE_URL=base_url, AGENT_TIMEOUT=timeout)


def __getattr__(name: str) -> str | int:
    if name in _SETTINGS_FIELDS:
        return getattr(load_config(), name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
