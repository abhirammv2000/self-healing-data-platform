"""get_circuit_breaker_state (tools.py) is the one real tool the diagnostic
agent can call. Tested here as a plain async function via its .ainvoke()
interface, independent of whether or when the model decides to call it
(that decision logic is tested in test_nodes.py instead).
"""
from types import SimpleNamespace
from unittest.mock import AsyncMock

from worker.app.agent import tools


class FakeSessionCM:
    def __init__(self, scalar_result):
        self._scalar_result = scalar_result

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    async def execute(self, *args, **kwargs):
        return SimpleNamespace(scalar_one_or_none=lambda: self._scalar_result)


def make_cb(**overrides):
    defaults = dict(state="closed", failure_count_threshold=3, failure_window_minutes=60, failure_reason=None, retry_after=None)
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


async def test_returns_a_descriptive_string_when_no_circuit_breaker_record_exists(monkeypatch):
    monkeypatch.setattr(tools, "async_session", lambda: FakeSessionCM(None))

    result = await tools.get_circuit_breaker_state.ainvoke({"pipeline_id": 42})

    assert "pipeline_id=42" in result
    assert "no prior failures" in result.lower()


async def test_reports_open_state_and_failure_reason(monkeypatch):
    cb = make_cb(state="open", failure_reason="3 failures in 60 min: ConnectionTimeout")
    monkeypatch.setattr(tools, "async_session", lambda: FakeSessionCM(cb))

    result = await tools.get_circuit_breaker_state.ainvoke({"pipeline_id": 7})

    assert "state=open" in result
    assert "3 failures in 60 min" in result


async def test_reports_retry_after_when_present(monkeypatch):
    import datetime
    cb = make_cb(state="open", retry_after=datetime.datetime(2026, 1, 1, 12, 0, 0))
    monkeypatch.setattr(tools, "async_session", lambda: FakeSessionCM(cb))

    result = await tools.get_circuit_breaker_state.ainvoke({"pipeline_id": 7})

    assert "retry_after=2026-01-01T12:00:00" in result


async def test_closed_state_omits_failure_reason_and_retry_after(monkeypatch):
    cb = make_cb(state="closed")
    monkeypatch.setattr(tools, "async_session", lambda: FakeSessionCM(cb))

    result = await tools.get_circuit_breaker_state.ainvoke({"pipeline_id": 7})

    assert "state=closed" in result
    assert "failure_reason" not in result
    assert "retry_after" not in result


async def test_a_db_error_returns_a_descriptive_string_not_an_exception(monkeypatch):
    class BrokenSessionCM:
        async def __aenter__(self):
            raise RuntimeError("connection refused")
        async def __aexit__(self, *args):
            return False

    monkeypatch.setattr(tools, "async_session", lambda: BrokenSessionCM())

    result = await tools.get_circuit_breaker_state.ainvoke({"pipeline_id": 7})

    assert "could not check" in result.lower()
    assert "connection refused" in result
