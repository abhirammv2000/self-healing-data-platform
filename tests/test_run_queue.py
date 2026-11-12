"""Putting a run on the queue: a failed push must never lose the run or raise to the caller."""
from types import SimpleNamespace
from unittest.mock import AsyncMock

from shared import run_queue


async def test_a_run_is_pushed_to_the_queue_and_noted(monkeypatch):
    pushed, noted = [], []

    async def rpush(queue, value):
        pushed.append((queue, value))

    async def mark(run_id):
        noted.append(run_id)

    monkeypatch.setattr(run_queue, "redis_client", SimpleNamespace(rpush=rpush))
    monkeypatch.setattr(run_queue, "mark_enqueued", mark)

    assert await run_queue.enqueue_run(42) is True
    assert pushed == [("pipeline_runs", "42")] and noted == [42]


async def test_a_push_that_fails_returns_false_and_does_not_raise(monkeypatch):
    noted = []

    monkeypatch.setattr(run_queue, "redis_client", SimpleNamespace(rpush=AsyncMock(side_effect=ConnectionError("down"))))
    monkeypatch.setattr(run_queue, "mark_enqueued", AsyncMock(side_effect=lambda run_id: noted.append(run_id)))

    assert await run_queue.enqueue_run(42) is False
    # nothing is noted, so last_enqueued_at stays empty and the reaper finds the run
    assert noted == []


async def test_failing_to_note_the_push_does_not_raise(monkeypatch):
    class BrokenSessionFactory:
        def __call__(self):
            raise RuntimeError("database down")

    monkeypatch.setattr(run_queue, "async_session", BrokenSessionFactory())

    await run_queue.mark_enqueued(42)  # must not raise: the id is on the queue, and a second copy of it is harmless
