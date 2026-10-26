"""SSE event payloads forwarded from the agent service."""

from dataclasses import asdict, dataclass
from typing import Any


@dataclass
class InProgressEvent:
    """Run is still in progress."""

    run_id: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class CompletedEvent:
    """Run finished. `response` shape is owned by the agent contract."""

    run_id: str
    response: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class FailedEvent:
    """Run failed."""

    run_id: str
    error: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
