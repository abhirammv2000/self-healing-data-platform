"""/health says the process is alive. /ready says it can serve: Postgres and Redis both answer."""
import asyncio
from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient

from control_plane.app import main as control_plane_main


@pytest.fixture
def client():
    return TestClient(control_plane_main.app)


def set_checks(monkeypatch, postgres, redis):
    monkeypatch.setattr(control_plane_main, "_check_postgres", postgres)
    monkeypatch.setattr(control_plane_main.redis_client, "ping", redis)


def test_ready_when_both_answer(client, monkeypatch):
    set_checks(monkeypatch, AsyncMock(), AsyncMock())
    response = client.get("/ready")
    assert response.status_code == 200
    assert response.json() == {"status": "ready", "checks": {"postgres": "ok", "redis": "ok"}}


def test_not_ready_when_postgres_is_down(client, monkeypatch):
    set_checks(monkeypatch, AsyncMock(side_effect=OSError("connection refused")), AsyncMock())
    response = client.get("/ready")
    assert response.status_code == 503
    assert response.json()["checks"] == {"postgres": "unavailable", "redis": "ok"}


def test_not_ready_when_redis_is_down(client, monkeypatch):
    set_checks(monkeypatch, AsyncMock(), AsyncMock(side_effect=ConnectionError("down")))
    response = client.get("/ready")
    assert response.status_code == 503
    assert response.json()["checks"] == {"postgres": "ok", "redis": "unavailable"}


def test_a_hung_dependency_is_not_ready_instead_of_hanging(client, monkeypatch):
    async def hangs():
        await asyncio.sleep(30)

    real_wait_for = asyncio.wait_for

    async def short_wait_for(awaitable, timeout):
        return await real_wait_for(awaitable, timeout=0.05)

    monkeypatch.setattr(control_plane_main.asyncio, "wait_for", short_wait_for)
    set_checks(monkeypatch, hangs, AsyncMock())
    response = client.get("/ready")
    assert response.status_code == 503
    assert response.json()["checks"]["postgres"] == "unavailable"


def test_the_error_text_is_not_sent_to_the_caller(client, monkeypatch):
    set_checks(monkeypatch, AsyncMock(side_effect=OSError("password for user x on host 10.0.3.4")), AsyncMock())
    assert "10.0.3.4" not in client.get("/ready").text


def test_health_does_not_look_at_any_dependency(client, monkeypatch):
    set_checks(monkeypatch, AsyncMock(side_effect=OSError("down")), AsyncMock(side_effect=OSError("down")))
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "healthy"}


def test_startup_stops_on_a_bad_configuration(monkeypatch):
    for name in ("API_KEY_SECRET", "ADMIN_SECRET_KEY"):
        monkeypatch.delenv(name, raising=False)
    with pytest.raises(RuntimeError, match="API_KEY_SECRET is not set"):
        with TestClient(control_plane_main.app):
            pass
