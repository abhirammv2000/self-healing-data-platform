"""An open circuit breaker recovers by itself: when its wait is over, one trial run decides.

Before this, a breaker stayed open until a person set it to half-open through the API (see docs/RELIABILITY.md).
The time in `retry_after` is written by the worker with the clock of the machine it runs on (shared.utils.now_naive),
so these tests set it relative to that same clock.
"""
from datetime import timedelta

import pytest

from shared.utils import now_naive
from worker.app.executor import run_executor_service

from tests.integration import helpers


async def _run(world, pipeline):
    run_id = await world.run(pipeline)
    try:
        await run_executor_service(run_id)
    except Exception:
        pass  # a failing step re-raises its error, and the test looks at the run
    return await helpers.get_run(run_id)


async def test_a_pipeline_that_fails_opens_its_breaker_and_recovers_when_the_wait_is_over(world):
    bad = await world.pipeline("/fail")
    await helpers.set_breaker(bad, failure_count_threshold=2)

    # two failing runs open the breaker, and the third run is blocked without running anything
    assert (await _run(world, bad)).status == "failed"
    assert (await _run(world, bad)).status == "failed"
    breaker = await helpers.get_breaker(bad)
    assert breaker.state == "open" and breaker.retry_after is not None
    assert (await _run(world, bad)).status == "blocked"

    # the wait is over, and the source has been fixed
    await helpers.set_breaker(bad, retry_after=now_naive() - timedelta(seconds=1))
    good_source = world.source + "/ok.csv"
    from control_plane.app.models.pipeline_steps import PipelineStep
    from shared.db import async_session
    from sqlalchemy import update

    async with async_session() as session:
        await session.execute(
            update(PipelineStep).where(PipelineStep.pipeline_id == bad).values(config={"source_url": good_source})
        )
        await session.commit()

    trial = await _run(world, bad)

    assert trial.status == "success"
    breaker = await helpers.get_breaker(bad)
    assert breaker.state == "closed" and breaker.retry_after is None and breaker.failure_reason is None


async def test_a_failed_trial_opens_the_breaker_again_for_a_full_wait(world):
    bad = await world.pipeline("/fail")
    await helpers.set_breaker(bad, state="open", failure_reason="old", retry_after=now_naive() - timedelta(seconds=1))

    trial = await _run(world, bad)

    assert trial.status == "failed"
    breaker = await helpers.get_breaker(bad)
    assert breaker.state == "open"
    assert breaker.retry_after > now_naive() + timedelta(minutes=50)  # the window is 60 minutes


async def test_a_breaker_that_is_still_waiting_blocks_and_stays_open(world):
    pipeline = await world.pipeline()
    await helpers.set_breaker(pipeline, state="open", retry_after=now_naive() + timedelta(minutes=5))

    run = await _run(world, pipeline)

    assert run.status == "blocked"
    assert (await helpers.get_breaker(pipeline)).state == "open"


async def test_a_breaker_opened_by_hand_without_a_retry_time_stays_open(world):
    pipeline = await world.pipeline()
    await helpers.set_breaker(pipeline, state="open", retry_after=None)

    assert (await _run(world, pipeline)).status == "blocked"
    assert (await helpers.get_breaker(pipeline)).state == "open"


@pytest.mark.parametrize("workers", [8])
async def test_runs_arriving_together_when_the_wait_ends_all_take_the_transition_safely(world, workers):
    import asyncio

    pipeline = await world.pipeline()
    await helpers.set_breaker(pipeline, state="open", retry_after=now_naive() - timedelta(seconds=1))
    run_ids = [await world.run(pipeline) for _ in range(workers)]

    await asyncio.gather(*(run_executor_service(run_id) for run_id in run_ids))

    # one run of the pipeline runs at a time, so one finished and the rest were left queued to wait their turn.
    # None of them was blocked by the breaker in the race
    runs = [await helpers.get_run(run_id) for run_id in run_ids]
    assert {run.status for run in runs} <= {"success", "queued"}
    assert "success" in {run.status for run in runs}
    assert (await helpers.get_breaker(pipeline)).state == "closed"

    await helpers.work()  # the waiting runs were put back on the queue as the pipeline became free
    runs = [await helpers.get_run(run_id) for run_id in run_ids]
    assert [run.status for run in runs] == ["success"] * workers


async def test_a_worker_that_lost_the_race_sees_the_breaker_the_winner_made_half_open(world):
    from shared.db import async_session
    from worker.app import executor

    from shared import metrics

    transitions = metrics.circuit_breaker_transitions_total.labels(to_state="half-open")
    before = transitions._value.get()
    pipeline = await world.pipeline()
    await helpers.set_breaker(pipeline, state="open", retry_after=now_naive() - timedelta(seconds=1))

    async with async_session() as loser:
        seen = await executor._fetch_circuit_breaker(loser, pipeline)
        assert seen.state == "open"  # the loser read the breaker before the winner moved it
        async with async_session() as winner:
            assert await executor._half_open_if_due(winner, await executor._fetch_circuit_breaker(winner, pipeline))

        assert await executor._half_open_if_due(loser, seen) is True
        assert seen.state == "half-open"
    # one transition happened, so one was counted: the loser's UPDATE changed nothing
    assert transitions._value.get() == before + 1
