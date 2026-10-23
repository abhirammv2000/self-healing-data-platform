"""The small helpers in worker/app/executor.py: retry delays, opening a circuit breaker, and the run record.

The retry delay and the circuit breaker are what the platform's retry and circuit breaker behavior rests on,
so each rule is checked on its own here. The run-level behavior is in test_executor_run.py.
"""
import json
from datetime import datetime
from types import SimpleNamespace

import pytest

from worker.app import executor
from worker.app.exceptions import ObservabilityRecordingError


# ---- calculate_delay ----


def test_exponential_backoff_doubles_with_each_attempt():
    delays = [executor.calculate_delay("exponential_backoff", 2, attempt) for attempt in (1, 2, 3, 4)]
    assert delays == [2, 4, 8, 16]


def test_a_missing_strategy_waits_the_base_delay_every_time():
    assert [executor.calculate_delay(None, 5, attempt) for attempt in (1, 2, 3)] == [5, 5, 5]


def test_an_unknown_strategy_is_treated_like_no_strategy():
    assert executor.calculate_delay("linear", 3, 4) == 3


def test_jitter_adds_between_half_and_all_of_the_exponential_delay(monkeypatch):
    seen = {}

    def fake_uniform(low, high):
        seen["range"] = (low, high)
        return high

    monkeypatch.setattr(executor.random, "uniform", fake_uniform)
    delay = executor.calculate_delay("exponential_backoff_jitter", 2, 3)
    # attempt 3 with base 2 is 8 seconds of exponential delay, and the jitter is drawn from 4 to 8
    assert seen["range"] == (4, 8)
    assert delay == 16


def test_jittered_delays_stay_inside_the_expected_band():
    for _ in range(200):
        delay = executor.calculate_delay("exponential_backoff_jitter", 1, 3)
        assert 6 <= delay <= 8


def test_a_zero_base_delay_never_waits():
    assert executor.calculate_delay("exponential_backoff", 0, 5) == 0
    assert executor.calculate_delay(None, 0, 5) == 0


# ---- mark_circuit_breaker_open ----


def test_opening_a_breaker_records_why_and_when_to_try_again(monkeypatch):
    now = datetime(2026, 1, 1, 12, 0, 0)
    monkeypatch.setattr(executor, "now_naive", lambda: now)
    breaker = SimpleNamespace(state="closed", failure_reason=None, retry_after=None, updated_at=None)
    retry_after = datetime(2026, 1, 1, 13, 0, 0)

    executor.mark_circuit_breaker_open(breaker, "Error Type:Timeout", retry_after)

    assert breaker.state == "open"
    assert breaker.failure_reason == "Error Type:Timeout"
    assert breaker.retry_after == retry_after
    assert breaker.updated_at == now


# ---- run_observability_recording ----


async def test_the_run_context_is_saved_as_json_under_the_run_id(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    context = {"run_id": 42, "run_status": "success", "step_attempts": [{"attempt": 1}]}

    await executor.run_observability_recording(context)

    saved = json.loads((tmp_path / "data" / "runs" / "42" / "run_context.json").read_text())
    assert saved == context


async def test_a_failed_write_is_reported_as_an_observability_error(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    def broken_open(*args, **kwargs):
        raise OSError("disk full")

    monkeypatch.setattr(executor, "open", broken_open, raising=False)

    with pytest.raises(ObservabilityRecordingError, match="disk full"):
        await executor.run_observability_recording({"run_id": 7})
