"""POST /query: forward a query and return the agent's immediate reply.

The agent owns timing and status. This handler generates a client `run_id`,
submits the query once, and registers `run_id` → `agent_run_id` after the
agent replies. It does not wait for completion, poll, or create run status.
"""

from __future__ import annotations

import logging
import uuid
from typing import Literal

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

logger = logging.getLogger(__name__)

router = APIRouter()

AGENT_UNAVAILABLE = "Agent service unavailable"
INTERNAL_ERROR = "Internal server error"

_AGENT_FAILURES = (AgentTimeoutError, AgentConnectionError, AgentHTTPError)


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
