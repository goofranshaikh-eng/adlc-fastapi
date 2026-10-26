"""Agent service configuration loading."""

import app.config as config


def test_agent_base_url_defaults_when_unset(monkeypatch) -> None:
    monkeypatch.delenv("AGENT_BASE_URL", raising=False)

    assert config.load_config().AGENT_BASE_URL == "http://localhost:8001"
    assert config.AGENT_BASE_URL == "http://localhost:8001"


def test_agent_base_url_loads_from_environment(monkeypatch) -> None:
    monkeypatch.setenv("AGENT_BASE_URL", "http://agent.internal:9000")

    assert config.load_config().AGENT_BASE_URL == "http://agent.internal:9000"
    assert config.AGENT_BASE_URL == "http://agent.internal:9000"


def test_agent_timeout_defaults_when_unset(monkeypatch) -> None:
    monkeypatch.delenv("AGENT_TIMEOUT", raising=False)

    settings = config.load_config()

    assert settings.AGENT_TIMEOUT == 3
    assert isinstance(settings.AGENT_TIMEOUT, int)
    assert config.AGENT_TIMEOUT == 3


def test_agent_timeout_loads_from_environment_as_integer(monkeypatch) -> None:
    monkeypatch.setenv("AGENT_TIMEOUT", "12")

    settings = config.load_config()

    assert settings.AGENT_TIMEOUT == 12
    assert isinstance(settings.AGENT_TIMEOUT, int)
    assert config.AGENT_TIMEOUT == 12
