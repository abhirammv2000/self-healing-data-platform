"""A schedule starts a run when its cron time comes, once, whoever checks and however often.

Before the scheduler existed the API stored cron expressions and nothing ever acted on them (see docs/RELIABILITY.md).
"""
import asyncio

from control_plane.app.schemas.schedules import ScheduleUpdate
from control_plane.app.services.schedules import update_schedule_service
from shared.db import async_session
from worker.app.reaper import requeue_unqueued_runs
from worker.app.scheduler import fire_due_schedules

from tests.integration import helpers

THREE_HOURS_AGO = "sql:now() - interval '3 hours'"


async def _due_schedule(world, cron="* * * * *", time_zone="UTC", path="/ok.csv"):
    """A pipeline with a schedule whose last handled time is three hours ago, so cron times have passed since."""
    pipeline = await world.pipeline(path)
    schedule_id = await helpers.make_schedule(world.tenant_id, pipeline, cron, time_zone)
    await helpers.set_schedule(schedule_id, handled_until=THREE_HOURS_AGO)
    return pipeline, schedule_id


async def test_a_due_schedule_starts_a_run_that_is_queued_and_finishes(world):
    pipeline, schedule_id = await _due_schedule(world)

    assert await fire_due_schedules() == 1

    (run,) = await helpers.runs_of(pipeline)
    assert run.status == "queued" and run.last_enqueued_at is not None
    assert run.idempotency_key.startswith(f"schedule:{schedule_id}:")
    assert await helpers.queue_contents() == [run.id]
    await helpers.work()
    assert (await helpers.get_run(run.id)).status == "success"


async def test_a_new_schedule_does_not_start_runs_for_times_before_it_existed(world):
    pipeline = await world.pipeline()
    await helpers.make_schedule(world.tenant_id, pipeline, "* * * * *")

    assert await fire_due_schedules() == 0
    assert await helpers.runs_of(pipeline) == []


async def test_checking_again_does_not_start_the_same_run_again(world):
    pipeline, schedule_id = await _due_schedule(world)

    assert await fire_due_schedules() == 1
    assert await fire_due_schedules() == 0

    assert len(await helpers.runs_of(pipeline)) == 1


async def test_many_workers_checking_at_once_start_one_run_for_one_cron_time(world):
    pipeline, _ = await _due_schedule(world)

    created = await asyncio.gather(*(fire_due_schedules() for _ in range(10)))

    assert sum(created) == 1
    (run,) = await helpers.runs_of(pipeline)
    assert await helpers.queue_contents() == [run.id]  # on the queue once, by the worker whose insert won


async def test_after_a_long_stop_one_run_is_started_not_one_per_missed_time(world):
    pipeline, _ = await _due_schedule(world, cron="* * * * *")  # three hours is 180 missed minutes

    assert await fire_due_schedules() == 1

    assert len(await helpers.runs_of(pipeline)) == 1


async def test_a_schedule_with_no_cron_time_since_the_mark_starts_nothing(world):
    # the mark is now, and 0 0 1 1 * (midnight on 1 January) cannot have come between now and now
    pipeline, schedule_id = await _due_schedule(world, cron="0 0 1 1 *")
    await helpers.set_schedule(schedule_id, handled_until="sql:now()")

    assert await fire_due_schedules() == 0
    assert await helpers.runs_of(pipeline) == []


async def test_an_inactive_schedule_starts_nothing(world):
    pipeline, schedule_id = await _due_schedule(world)
    await helpers.set_schedule(schedule_id, is_active=False)

    assert await fire_due_schedules() == 0
    assert await helpers.runs_of(pipeline) == []


async def test_switching_a_paused_schedule_on_does_not_start_a_run_for_the_time_it_was_paused(world):
    pipeline, schedule_id = await _due_schedule(world)
    await helpers.set_schedule(schedule_id, is_active=False)

    async with async_session() as session:
        await update_schedule_service(world.tenant_id, pipeline, ScheduleUpdate(is_active=True), session)

    assert await fire_due_schedules() == 0
    assert await helpers.runs_of(pipeline) == []


async def test_changing_the_cron_expression_starts_from_now_not_from_the_old_mark(world):
    pipeline, schedule_id = await _due_schedule(world)

    async with async_session() as session:
        await update_schedule_service(world.tenant_id, pipeline, ScheduleUpdate(cron_expression="*/5 * * * *"), session)

    assert await fire_due_schedules() == 0


async def test_an_update_that_changes_nothing_about_timing_leaves_the_mark_alone(world):
    pipeline, schedule_id = await _due_schedule(world)
    before = (await helpers.get_schedule(schedule_id)).handled_until

    async with async_session() as session:
        await update_schedule_service(world.tenant_id, pipeline, ScheduleUpdate(cron_expression="* * * * *"), session)

    assert (await helpers.get_schedule(schedule_id)).handled_until == before


async def test_a_worker_that_stopped_after_creating_the_run_leaves_it_for_the_next_check_and_the_reaper(world):
    pipeline, schedule_id = await _due_schedule(world)
    assert await fire_due_schedules() == 1
    (run,) = await helpers.runs_of(pipeline)
    # play a worker that created the run and stopped before it recorded the fire time and before it queued the run
    await helpers.set_schedule(schedule_id, handled_until=THREE_HOURS_AGO)
    await helpers.set_run(run.id, last_enqueued_at=None)
    await helpers.redis_clear_queue()

    assert await fire_due_schedules() == 0  # the run for that fire time exists, so no second one
    assert len(await helpers.runs_of(pipeline)) == 1
    assert await requeue_unqueued_runs() == 1  # and the reaper queues it
    await helpers.work()
    assert (await helpers.get_run(run.id)).status == "success"


async def test_a_schedule_that_cannot_be_read_does_not_stop_the_others(world):
    broken_pipeline, broken_id = await _due_schedule(world)
    good_pipeline, _ = await _due_schedule(world)
    await helpers.set_schedule(broken_id, timezone="Not/AZone")

    assert await fire_due_schedules() == 1

    assert await helpers.runs_of(broken_pipeline) == []
    assert len(await helpers.runs_of(good_pipeline)) == 1


async def test_a_schedule_row_from_before_the_mark_existed_starts_from_now(world):
    pipeline, schedule_id = await _due_schedule(world)
    await helpers.set_schedule(schedule_id, handled_until=None)

    assert await fire_due_schedules() == 0
    assert (await helpers.get_schedule(schedule_id)).handled_until is not None
    assert await helpers.runs_of(pipeline) == []
