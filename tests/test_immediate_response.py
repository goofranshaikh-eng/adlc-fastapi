"""Immediate response schema serialization."""

import json

from app.models.query_models import ImmediateResponse

RUN_ID = "123e4567-e89b-12d3-a456-426614174000"


def test_ok_response_serializes() -> None:
    payload = ImmediateResponse(status="ok", run_id=RUN_ID).model_dump()

    assert json.dumps(payload) == json.dumps({"status": "ok", "run_id": RUN_ID})


def test_rejected_response_serializes() -> None:
    payload = ImmediateResponse(status="rejected", run_id=RUN_ID).model_dump()

    assert json.dumps(payload) == json.dumps(
        {"status": "rejected", "run_id": RUN_ID}
    )
