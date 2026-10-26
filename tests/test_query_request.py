"""Query request schema validation."""

import pytest
from pydantic import ValidationError

from app.models.query_models import QueryRequest


def test_valid_query_is_accepted() -> None:
    request = QueryRequest.model_validate({"query": "test query"})

    assert request.query == "test query"


def test_missing_query_is_rejected() -> None:
    with pytest.raises(ValidationError) as exc_info:
        QueryRequest.model_validate({})

    assert "query" in str(exc_info.value)


def test_empty_query_is_rejected() -> None:
    with pytest.raises(ValidationError) as exc_info:
        QueryRequest.model_validate({"query": ""})

    assert "query" in str(exc_info.value)
