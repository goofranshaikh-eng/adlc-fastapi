"""Query routes.

POST /query forwards a query and returns the agent's immediate reply. It
generates a client `run_id`, submits the query once, and registers
`run_id` → `agent_run_id`. GET /query/{run_id}/stream subscribes once to that
agent run and forwards events unchanged. The agent owns timing and status.
These handlers do not poll or delay. A `failed` event is sent only when the
agent subscription times out or fails.
"""

from __future__ import annotations

import json
import logging
import uuid
from collections.abc import AsyncIterator
from typing import Any, Literal

from fastapi import APIRouter, Depends, HTTPException

from app.dependencies import get_agent_client
from app.models.query_models import ImmediateResponse, QueryRequest
from app.services.agent_client import (
    AgentClient,
    AgentConnectionError,
    AgentHTTPError,
    AgentTimeoutError,
)
from app.services.run_registry import RunRegistry, get_run_registry
from app.streaming import SSEResponse

logger = logging.getLogger(__name__)

router = APIRouter()

AGENT_UNAVAILABLE = "Agent service unavailable"
INTERNAL_ERROR = "Internal server error"
RUN_NOT_FOUND = "Run not found"
AGENT_TIMEOUT = "Agent timeout"
STREAM_ERROR = "Stream error"

_AGENT_FAILURES = (AgentTimeoutError, AgentConnectionError, AgentHTTPError)
_TERMINAL_EVENTS = frozenset({"completed", "failed"})


@router.post("/query", response_model=ImmediateResponse, status_code=200)
async def submit_query(
    body: QueryRequest,
    agent_client: AgentClient = Depends(get_agent_client),
    registry: RunRegistry = Depends(get_run_registry),
) -> ImmediateResponse:
    """Submit a query and return the agent's immediate ok or rejected reply."""
    run_id = str(uuid.uuid4())
    try:
        payload = await agent_client.initiate_query(body.model_dump())
    except _AGENT_FAILURES as exc:
        logger.exception("agent service unavailable")
        raise HTTPException(status_code=503, detail=AGENT_UNAVAILABLE) from exc
    except Exception as exc:
        logger.exception("unexpected error submitting query")
        raise HTTPException(status_code=500, detail=INTERNAL_ERROR) from exc

    try:
        reply_status, agent_run_id = _require_agent_reply(payload)
        registry.register(run_id=run_id, agent_run_id=agent_run_id)
    except Exception as exc:
        logger.exception("unexpected error registering run")
        raise HTTPException(status_code=500, detail=INTERNAL_ERROR) from exc

    return ImmediateResponse(status=reply_status, run_id=run_id)


def _require_agent_reply(payload: object) -> tuple[Literal["ok", "rejected"], str]:
    """Read status and agent_run_id from the agent's immediate JSON reply."""
    if not isinstance(payload, dict):
        raise ValueError("agent reply must be a JSON object")
    reply_status = payload.get("status")
    agent_run_id = payload.get("agent_run_id")
    if reply_status not in ("ok", "rejected"):
        raise ValueError("agent reply status is invalid")
    if not isinstance(agent_run_id, str) or agent_run_id == "":
        raise ValueError("agent reply is missing agent_run_id")
    if reply_status == "ok":
        return "ok", agent_run_id
    return "rejected", agent_run_id


@router.get("/query/{run_id}/stream")
async def stream_status(
    run_id: str,
    agent_client: AgentClient = Depends(get_agent_client),
    registry: RunRegistry = Depends(get_run_registry),
) -> SSEResponse:
    """Forward one agent event stream, then drop the local run mapping.

    Missing runs are 404. After the stream is open, a subscription timeout or
    a drop during a non-terminal event is a backend-generated `failed` event.
    That recovery is not an agent-forwarded status and is not retried.
    """
    try:
        agent_run_id = registry.lookup(run_id)
    except KeyError:
        raise HTTPException(status_code=404, detail=RUN_NOT_FOUND) from None

    return SSEResponse(_forward_events(run_id, agent_run_id, agent_client, registry))


async def _forward_events(
    run_id: str,
    agent_run_id: str,
    agent_client: AgentClient,
    registry: RunRegistry,
) -> AsyncIterator[str]:
    """Yield agent SSE frames until the first terminal event.

    Exactly one of ``completed`` or ``failed`` is forwarded. The stream then
    closes, so a malformed agent stream cannot deliver both. The ``finally``
    block drops the local run mapping and does not cancel the agent run.
    """
    try:
        async for agent_event in agent_client.subscribe_to_run(agent_run_id):
            event_type = agent_event["event"]
            event_data = agent_event["data"]
            yield _sse_frame(event_type, event_data)
            # AD-4: the first completed or failed event closes the stream.
            # Anything the agent sends after that, including a second terminal
            # event, is left unread.
            if event_type in _TERMINAL_EVENTS:
                break
    except TimeoutError:
        # Backend-generated exception event. AC3 still forbids inventing
        # status on the normal path; this runs only when the subscription
        # times out (HLD Risk 3), and it is not an event the agent sent.
        logger.exception("agent subscription timed out")
        yield _sse_frame("failed", {"run_id": run_id, "error": AGENT_TIMEOUT})
    except Exception:
        # Backend-generated exception event for a failed subscription,
        # including a disconnect while the run is still in_progress.
        # One shot: the subscription is not retried (HLD Risk 3).
        logger.exception("agent subscription failed")
        yield _sse_frame("failed", {"run_id": run_id, "error": STREAM_ERROR})
    finally:
        # Local id mapping only. The agent's background job continues.
        try:
            registry.remove(run_id)
        except KeyError:
            pass


def _sse_frame(event_type: str, data: Any) -> str:
    """Serialize one SSE event exactly as `event` plus a JSON `data` line."""
    return f"event: {event_type}\ndata: {json.dumps(data)}\n\n"
