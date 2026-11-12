"""A worker that lost its lease cannot overwrite the worker that took the run over.

A worker is not always dead when its lease runs out. It can be paused (a long garbage collection, a stalled network, a
laptop lid) and wake up later, still holding a run that another worker has since finished. Without a check, its late
write wins. Each claim of a run gets a number (`attempts`), and every write by a worker is checked against it. Before
this existed, the first test below ended with the run marked failed although the second worker had finished it (see
docs/RELIABILITY.md).
"""
import asyncio

from worker.app import executor
from worker.app.executor import run_executor_service
from worker.app.lease import LeaseKeeper, extend_lease
from worker.app.reaper import recover_dead_runs, requeue_unqueued_runs

from tests.integration import helpers
from tests.integration.conftest import CSV


class NoHeartbeat:
    """A worker that is paused: it sends no sign of life."""

    lost = False

    async def stop(self):
        pass


async def _run_until_running(run_id):
    for _ in range(60):
        if (await helpers.get_run(run_id)).status == "running":
            return
        await asyncio.sleep(0.1)
    raise AssertionError("the run never started")


async def test_a_paused_worker_that_wakes_up_and_fails_does_not_overwrite_the_worker_that_finished_the_run(
    world, monkeypatch
):
    calls = {"n": 0}

    async def fetch_data(url):
        calls["n"] += 1
        if calls["n"] == 1:  # the paused worker: slow, and in the end it fails
            await asyncio.sleep(2.5)
            raise ConnectionError("the paused worker finally fails")
        return CSV

    monkeypatch.setattr("worker.app.step_handlers.fetch_data", fetch_data)
    monkeypatch.setattr(executor, "start_lease_keeper", lambda *args: NoHeartbeat())
    monkeypatch.setattr(executor, "RUN_LEASE_SECONDS", 1)
    pipeline = await world.pipeline()
    run_id = await world.run(pipeline)

    paused = asyncio.create_task(run_executor_service(run_id))
    await _run_until_running(run_id)
    await asyncio.sleep(1.3)  # the lease runs out while the worker is only paused
    assert await recover_dead_runs() == (1, 0)
    assert await requeue_unqueued_runs() == 1
    await helpers.work(wait=0.5)  # a second worker takes the run and finishes it
    assert (await helpers.get_run(run_id)).status == "success"

    await paused  # the paused worker wakes up, fails, and must change nothing

    run = await helpers.get_run(run_id)
    assert run.status == "success" and run.attempts == 2
    assert run.error_type is None and run.error_message is None
    breaker = await helpers.get_breaker(pipeline)
    assert breaker.state == "closed"


async def test_a_paused_worker_that_wakes_up_and_succeeds_does_not_touch_the_run_either(world, monkeypatch):
    calls = {"n": 0}

    async def fetch_data(url):
        calls["n"] += 1
        if calls["n"] == 1:
            await asyncio.sleep(2.0)
        return CSV

    monkeypatch.setattr("worker.app.step_handlers.fetch_data", fetch_data)
    monkeypatch.setattr(executor, "start_lease_keeper", lambda *args: NoHeartbeat())
    monkeypatch.setattr(executor, "RUN_LEASE_SECONDS", 1)
    pipeline = await world.pipeline()
    run_id = await world.run(pipeline)

    paused = asyncio.create_task(run_executor_service(run_id))
    await _run_until_running(run_id)
    await asyncio.sleep(1.3)
    await recover_dead_runs()
    await requeue_unqueued_runs()
    # the second worker takes the run and is still working when the first wakes up and finishes
    second = asyncio.create_task(helpers.work(wait=0.5))
    await paused
    await second

    run = await helpers.get_run(run_id)
    assert run.status == "success" and run.attempts == 2
    assert run.ended_at is not None


async def test_a_worker_that_lost_the_run_cannot_extend_the_lease_of_the_worker_that_has_it(world):
    pipeline = await world.pipeline()
    run_id = await world.run(pipeline)
    # the second worker's claim: attempt 2, lease running
    await helpers.set_run(run_id, status="running", attempts=2, lease_expires_at="sql:now() + interval '5 seconds'")
    before = (await helpers.get_run(run_id)).lease_expires_at

    assert await extend_lease(run_id, attempt=1, lease_seconds=60) is False  # the first worker's late heartbeat
    assert (await helpers.get_run(run_id)).lease_expires_at == before
    assert await extend_lease(run_id, attempt=2, lease_seconds=60) is True  # the owner's heartbeat still works
    assert (await helpers.get_run(run_id)).lease_expires_at > before


async def test_the_heartbeat_of_a_worker_that_lost_the_run_says_so_and_stops(world):
    pipeline = await world.pipeline()
    run_id = await world.run(pipeline)
    await helpers.set_run(run_id, status="running", attempts=2, lease_expires_at="sql:now() + interval '1 minute'")

    keeper = LeaseKeeper(run_id, attempt=1, lease_seconds=30, beat_seconds=0.2).start()
    for _ in range(20):
        if keeper.lost:
            break
        await asyncio.sleep(0.1)
    await keeper.stop()

    assert keeper.lost is True


async def test_the_owner_of_a_run_is_never_told_it_lost_it(world):
    pipeline = await world.pipeline()
    run_id = await world.run(pipeline)
    await helpers.set_run(run_id, status="running", attempts=1, lease_expires_at="sql:now() + interval '1 minute'")

    keeper = LeaseKeeper(run_id, attempt=1, lease_seconds=30, beat_seconds=0.1).start()
    await asyncio.sleep(0.6)
    await keeper.stop()

    assert keeper.lost is False
