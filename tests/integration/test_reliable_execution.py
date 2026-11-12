"""A run is never lost: not when the queue is down, not when its id is dropped, not when its worker dies.

These play out the failures on a real Postgres and a real Redis. Before the lease and the reaper existed, each of the
first four left a run stuck for ever (see docs/RELIABILITY.md).
"""
import asyncio
from unittest.mock import AsyncMock

from shared import run_queue
from worker.app.lease import LeaseKeeper
from worker.app.reaper import recover_dead_runs, requeue_unqueued_runs

from tests.integration import helpers


async def test_a_run_created_while_redis_is_down_is_started_once_redis_is_back(world, monkeypatch):
    pipeline = await world.pipeline()
    run_id = await world.run(pipeline)

    # Redis is down: the push fails, and the caller is told the run exists, not that it failed
    with monkeypatch.context() as down:  # only this patch is undone, the fetch stub of the fixtures stays
        down.setattr(run_queue.redis_client, "rpush", AsyncMock(side_effect=ConnectionError("redis is down")))
        assert await run_queue.enqueue_run(run_id) is False
    assert (await helpers.get_run(run_id)).last_enqueued_at is None
    assert await helpers.queue_contents() == []

    # Redis is back, and the reaper finds the run
    assert await requeue_unqueued_runs() == 1
    assert await helpers.queue_contents() == [run_id]

    await helpers.work()
    run = await helpers.get_run(run_id)
    assert run.status == "success" and run.attempts == 1


async def test_a_run_whose_id_was_dropped_from_the_queue_is_put_back(world):
    pipeline = await world.pipeline()
    run_id = await world.run(pipeline)
    # it was queued a minute ago, and a worker took the id off the queue and stopped before it claimed the run
    await helpers.set_run(run_id, last_enqueued_at="sql:now() - interval '2 minutes'")

    assert await requeue_unqueued_runs() == 1
    await helpers.work()

    assert (await helpers.get_run(run_id)).status == "success"


async def test_a_run_that_was_queued_a_moment_ago_is_not_pushed_again(world):
    pipeline = await world.pipeline()
    run_id = await world.run(pipeline)
    await helpers.set_run(run_id, last_enqueued_at="sql:now()")

    assert await requeue_unqueued_runs() == 0
    assert await helpers.queue_contents() == []


async def test_a_run_whose_worker_died_is_taken_over_and_finished(world):
    pipeline = await world.pipeline()
    run_id = await world.run(pipeline)
    # a worker claimed it, ran for a while and then died: the status says running and the lease has passed
    await helpers.set_run(run_id, status="running", attempts=1, lease_expires_at="sql:now() - interval '1 minute'")

    assert await recover_dead_runs() == (1, 0)
    assert (await helpers.get_run(run_id)).status == "queued"
    assert await requeue_unqueued_runs() == 1
    await helpers.work()

    run = await helpers.get_run(run_id)
    assert run.status == "success"
    assert run.attempts == 2  # the first claim, and the one after the takeover


async def test_a_run_that_keeps_losing_its_worker_is_failed_not_tried_for_ever(world):
    pipeline = await world.pipeline()
    run_id = await world.run(pipeline)
    await helpers.set_run(run_id, status="running", attempts=3, lease_expires_at="sql:now() - interval '1 minute'")

    assert await recover_dead_runs(max_attempts=3) == (0, 1)

    run = await helpers.get_run(run_id)
    assert run.status == "failed" and run.error_type == "worker_lost" and run.ended_at is not None
    assert await requeue_unqueued_runs() == 0


async def test_a_run_with_a_live_lease_is_left_alone(world):
    pipeline = await world.pipeline()
    run_id = await world.run(pipeline)
    await helpers.set_run(run_id, status="running", attempts=1, lease_expires_at="sql:now() + interval '1 minute'")

    assert await recover_dead_runs() == (0, 0)
    assert (await helpers.get_run(run_id)).status == "running"


async def test_the_heartbeat_extends_the_lease_while_the_run_works_and_stops_when_it_ends(world):
    pipeline = await world.pipeline()
    run_id = await world.run(pipeline)
    await helpers.set_run(run_id, status="running", attempts=1, lease_expires_at="sql:now() + interval '2 seconds'")
    before = (await helpers.get_run(run_id)).lease_expires_at

    keeper = LeaseKeeper(run_id, attempt=1, lease_seconds=30, beat_seconds=0.2).start()
    await asyncio.sleep(0.8)
    extended = (await helpers.get_run(run_id)).lease_expires_at
    assert (extended - before).total_seconds() > 20

    # the run ends. A heartbeat that fires after that must not touch it
    await helpers.set_run(run_id, status="success", lease_expires_at="sql:now()")
    ended_at = (await helpers.get_run(run_id)).lease_expires_at
    await asyncio.sleep(0.6)
    await keeper.stop()
    assert (await helpers.get_run(run_id)).lease_expires_at == ended_at


async def test_a_real_run_holds_a_lease_while_it_runs(world):
    pipeline = await world.pipeline("/slow.csv?secs=2")
    run_id = await world.run(pipeline)
    await run_queue.enqueue_run(run_id)

    worker = asyncio.create_task(helpers.work(wait=0.5))
    for _ in range(40):
        run = await helpers.get_run(run_id)
        if run.status == "running":
            break
        await asyncio.sleep(0.1)
    assert run.status == "running" and run.attempts == 1 and run.lease_expires_at is not None
    await worker

    assert (await helpers.get_run(run_id)).status == "success"


async def test_the_same_id_many_times_runs_the_run_once(world):
    pipeline = await world.pipeline("/slow.csv?secs=1")
    run_id = await world.run(pipeline)
    for _ in range(20):
        await run_queue.redis_client.rpush(run_queue.QUEUE_NAME, str(run_id))

    already_handled = await helpers.work(workers=4)

    run = await helpers.get_run(run_id)
    assert run.status == "success" and run.attempts == 1
    assert already_handled == 19


async def test_many_runs_on_many_workers_each_run_exactly_once(world):
    pipeline = await world.pipeline()
    run_ids = [await world.run(pipeline) for _ in range(40)]
    for run_id in run_ids:
        await run_queue.enqueue_run(run_id)

    await helpers.work(workers=6)

    runs = [await helpers.get_run(run_id) for run_id in run_ids]
    assert [run.status for run in runs] == ["success"] * 40
    assert [run.attempts for run in runs] == [1] * 40


async def test_several_reapers_at_once_put_each_run_on_the_queue_once(world):
    pipeline = await world.pipeline()
    run_ids = [await world.run(pipeline) for _ in range(50)]

    pushed = await asyncio.gather(*(requeue_unqueued_runs() for _ in range(5)))

    queued = await helpers.queue_contents()
    assert sum(pushed) == 50
    assert sorted(queued) == sorted(run_ids)  # every run once, none twice
