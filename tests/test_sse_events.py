"""SSE event schema instantiation and JSON shape."""

import json

from app.models.sse_events import CompletedEvent, FailedEvent, InProgressEvent

RUN_ID = "123e4567-e89b-12d3-a456-426614174000"


def test_in_progress_event() -> None:
    event = InProgressEvent(run_id=RUN_ID)

    assert event.run_id == RUN_ID
    assert json.dumps(event.to_dict()) == json.dumps({"run_id": RUN_ID})


def test_completed_event() -> None:
    response = {"answer": "done"}
    event = CompletedEvent(run_id=RUN_ID, response=response)

    assert event.run_id == RUN_ID
    assert event.response == response
    assert json.dumps(event.to_dict()) == json.dumps(
        {"run_id": RUN_ID, "response": response}
    )


def test_failed_event() -> None:
    event = FailedEvent(run_id=RUN_ID, error="agent failed")

    assert event.run_id == RUN_ID
    assert event.error == "agent failed"
    assert json.dumps(event.to_dict()) == json.dumps(
        {"run_id": RUN_ID, "error": "agent failed"}
    )
