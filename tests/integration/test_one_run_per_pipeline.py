"""Only one run of a pipeline is running at a time, whichever workers try, and other pipelines are not held back.

The rule is a unique index on pipeline_id for runs that are 'running' or 'retrying'. A worker that cannot claim a run
for that reason leaves it queued and the reaper offers it again, so runs of one pipeline go one after the other.
Without this a schedule whose run takes longer than its interval overlapped with the next run, and two runs wrote the
same warehouse table at once (see docs/RELIABILITY.md).
"""
import asyncio

from sqlalchemy import text

from shared import run_queue
from shared.db import async_session
from shared.run_queue import wake_waiting_run
from worker.app.reaper import recover_dead_runs, requeue_unqueued_runs

from tests.integration import helpers


async def _active_runs_of(pipeline_id):
    async with async_session() as session:
        return (
            await session.execute(
                text("select count(*) from pipeline_runs where pipeline_id = :p and status in ('running', 'retrying')"),
                {"p": pipeline_id},
            )
        ).scalar_one()


async def _watch(pipelines, stop, seen):
    """Note the most runs of any one pipeline that were active at the same moment."""
    while not stop.is_set():
        for pipeline in pipelines:
            seen["max"] = max(seen["max"], await _active_runs_of(pipeline))
        await asyncio.sleep(0.05)


async def test_a_second_run_of_a_pipeline_waits_while_the_first_is_running_and_then_runs(world):
    pipeline = await world.pipeline("/slow.csv?secs=2")
    first = await world.run(pipeline)
    second = await world.run(pipeline)
    await run_queue.enqueue_run(first)
    await run_queue.enqueue_run(second)
    stop, seen = asyncio.Event(), {"max": 0}
    watcher = asyncio.create_task(_watch([pipeline], stop, seen))
    seen_waiting = False

    async def look_for_a_waiting_run():
        nonlocal seen_waiting
        for _ in range(40):
            statuses = sorted([(await helpers.get_run(first)).status, (await helpers.get_run(second)).status])
            if statuses == ["queued", "running"]:
                seen_waiting = True
                return
            await asyncio.sleep(0.1)

    # two workers take the two ids at the same moment: one runs, the other leaves its run queued
    await asyncio.gather(helpers.work(workers=2, wait=2.0), look_for_a_waiting_run())

    stop.set()
    await watcher
    assert seen_waiting  # for a while one run was running and the other was waiting
    assert seen["max"] == 1  # and the two were never running at once
    runs = [await helpers.get_run(first), await helpers.get_run(second)]
    # the waiting run was put on the queue the moment the first ended, without the reaper, and used one attempt
    assert [run.status for run in runs] == ["success", "success"]
    assert [run.attempts for run in runs] == [1, 1]


async def test_runs_of_different_pipelines_are_not_held_back_by_each_other(world):
    one = await world.pipeline("/slow.csv?secs=2")
    two = await world.pipeline("/slow.csv?secs=2")
    runs = [await world.run(one), await world.run(two)]
    for run_id in runs:
        await run_queue.enqueue_run(run_id)

    worker = asyncio.create_task(helpers.work(workers=2, wait=0.5))
    both_running = False
    for _ in range(40):
        statuses = [(await helpers.get_run(run_id)).status for run_id in runs]
        if statuses == ["running", "running"]:
            both_running = True
            break
        await asyncio.sleep(0.1)
    await worker

    assert both_running
    assert [(await helpers.get_run(run_id)).status for run_id in runs] == ["success", "success"]


async def test_many_runs_of_one_pipeline_on_many_workers_never_overlap_and_all_finish(world):
    pipeline = await world.pipeline("/slow.csv?secs=0.3")
    run_ids = [await world.run(pipeline) for _ in range(10)]
    for run_id in run_ids:
        await run_queue.enqueue_run(run_id)
    stop, seen = asyncio.Event(), {"max": 0}
    watcher = asyncio.create_task(_watch([pipeline], stop, seen))

    # one pass and no reaper: when a run ends, the next waiting run of the pipeline is put on the queue at once
    await helpers.work(workers=6, wait=2.0)

    stop.set()
    await watcher
    runs = [await helpers.get_run(run_id) for run_id in run_ids]
    assert [run.status for run in runs] == ["success"] * 10
    assert [run.attempts for run in runs] == [1] * 10  # waiting did not cost an attempt, and none ran twice
    assert seen["max"] == 1


async def test_a_run_that_is_retrying_also_holds_its_pipeline(world):
    pipeline = await world.pipeline()
    holder = await world.run(pipeline)
    waiting = await world.run(pipeline)
    await helpers.set_run(holder, status="retrying", attempts=1, lease_expires_at="sql:now() + interval '1 minute'")
    await run_queue.enqueue_run(waiting)

    await helpers.work()

    assert (await helpers.get_run(waiting)).status == "queued"


async def test_the_slot_of_a_dead_worker_is_free_again_once_the_reaper_takes_the_run_back(world):
    pipeline = await world.pipeline()
    dead = await world.run(pipeline)
    waiting = await world.run(pipeline)
    await helpers.set_run(dead, status="running", attempts=1, lease_expires_at="sql:now() - interval '1 minute'")
    await run_queue.enqueue_run(waiting)

    await helpers.work()
    assert (await helpers.get_run(waiting)).status == "queued"  # the dead worker's run still holds the pipeline

    assert await recover_dead_runs() == (1, 0)
    for run_id in (dead, waiting):
        await helpers.set_run(run_id, last_enqueued_at=None)
    await requeue_unqueued_runs()
    for _ in range(5):  # the two runs go one after the other
        await helpers.work(wait=0.3)
        statuses = [(await helpers.get_run(run_id)).status for run_id in (dead, waiting)]
        if statuses == ["success", "success"]:
            break
        for run_id in (dead, waiting):
            await helpers.set_run(run_id, last_enqueued_at="sql:now() - interval '1 minute'")
        await requeue_unqueued_runs()

    assert [(await helpers.get_run(run_id)).status for run_id in (dead, waiting)] == ["success", "success"]


async def test_a_run_that_had_to_wait_is_not_offered_again_straight_away(world):
    pipeline = await world.pipeline()
    holder = await world.run(pipeline)
    waiting = await world.run(pipeline)
    await helpers.set_run(holder, status="running", attempts=1, lease_expires_at="sql:now() + interval '1 minute'")
    # it was put on the queue long ago and a worker has just taken the id off
    await helpers.set_run(waiting, last_enqueued_at="sql:now() - interval '5 minutes'")
    await run_queue.redis_client.rpush(run_queue.QUEUE_NAME, str(waiting))

    await helpers.work()

    assert (await helpers.get_run(waiting)).status == "queued"
    assert await requeue_unqueued_runs() == 0  # it was just offered, so it waits ENQUEUE_VISIBILITY_SECONDS


async def test_waking_puts_the_oldest_waiting_run_of_the_pipeline_on_the_queue(world):
    pipeline = await world.pipeline()
    other = await world.pipeline()
    await world.run(other)  # a run of another pipeline, the oldest of all, is not woken
    oldest = await world.run(pipeline)
    await world.run(pipeline)

    assert await wake_waiting_run(pipeline) is True

    assert await helpers.queue_contents() == [oldest]
    assert (await helpers.get_run(oldest)).last_enqueued_at is not None


async def test_waking_a_pipeline_with_nothing_waiting_does_nothing(world):
    pipeline = await world.pipeline()
    done = await world.run(pipeline)
    await helpers.set_run(done, status="success")

    assert await wake_waiting_run(pipeline) is False
    assert await helpers.queue_contents() == []


async def test_a_lost_wake_up_costs_time_not_the_run(world, monkeypatch):
    # Redis is down at the moment the run ends: the waiting run is left for the reaper
    from unittest.mock import AsyncMock

    pipeline = await world.pipeline()
    waiting = await world.run(pipeline)
    with monkeypatch.context() as down:  # only this patch is undone, the fetch stub of the fixtures stays
        down.setattr(run_queue.redis_client, "rpush", AsyncMock(side_effect=ConnectionError("redis is down")))
        assert await wake_waiting_run(pipeline) is False

    assert await requeue_unqueued_runs() == 1
    await helpers.work()
    assert (await helpers.get_run(waiting)).status == "success"


async def test_a_database_error_while_waking_is_contained(world, monkeypatch):
    # the worker has just finished a run: a failure to look for the next one must not become the run's problem
    def broken():
        raise ConnectionError("the database is gone")

    monkeypatch.setattr(run_queue, "async_session", broken)

    assert await wake_waiting_run(1) is False
