"""Starts a run when a pipeline's cron time comes.

Schedules used to be stored and never read: the API accepted a cron expression and nothing ever acted on it. This is
the part that acts on it. Every worker runs this loop. There is no leader, because running it twice at once is safe:

  * The run for one fire time is created with the idempotency key 'schedule:<schedule id>:<fire time>'. The database
    has a unique constraint on (pipeline, key), so however many workers try, exactly one run is created for that time.
    Only the worker whose insert won puts the run on the queue.
  * If the worker that created the run stops before it queues it, the reaper (worker/app/reaper.py) queues it.
  * If the worker stops before it records the fire time as handled, the next sweep works out the same fire time, finds
    the run that already exists and records it.

What counts as due. A schedule row says 'handled_until': every fire time up to then is dealt with. A schedule is due
when its latest fire time that is not in the future is after handled_until. If several fire times were missed (all
workers were down for an hour of a every-minute schedule) one run is started for the latest of them and the rest are
passed over, since running a sync fifty times in a row to catch up helps nobody. Nothing is started for times before
the schedule was created, changed or switched on (the control plane sets handled_until then).

Fire times are worked out in the schedule's own time zone. The clock is the database's now(), so the machines running
workers need no synchronised clocks. A cron time is met within SCHEDULER_INTERVAL_SECONDS.

A run that takes longer than the gap between fire times overlaps with the next one. Nothing here stops that.
"""
import asyncio
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from croniter import croniter
from sqlalchemy import func, select, update

from control_plane.app.models.pipelines import Pipeline
from control_plane.app.models.schedules import Schedule
from control_plane.app.services.pipeline_runs import create_pipeline_run_service
from shared import metrics
from shared.config import SCHEDULER_INTERVAL_SECONDS
from shared.db import async_session
from shared.observability import get_logger
from shared.run_queue import enqueue_run

log = get_logger(__name__)


def latest_fire_time(cron_expression: str, time_zone: str, handled_until: datetime, now: datetime) -> datetime | None:
    """The newest fire time that is after handled_until and not after now, or None when there is none.

    Both times must carry a time zone. The answer is in the schedule's time zone.
    """
    zone = ZoneInfo(time_zone)
    # get_prev gives the newest fire time strictly before the time it starts from, so start a moment after now
    # to count a fire time that is exactly now
    newest = croniter(cron_expression, (now + timedelta(microseconds=1)).astimezone(zone)).get_prev(datetime)
    return newest if newest > handled_until else None


def idempotency_key(schedule_id: int, fire_time: datetime) -> str:
    # in UTC so the key is the same whatever time zone the schedule uses or is changed to
    return f"schedule:{schedule_id}:{fire_time.astimezone(timezone.utc).isoformat()}"


async def fire_due_schedules() -> int:
    """Start a run for every active schedule that is due. Returns how many runs this call created."""
    async with async_session() as session:
        now = (await session.execute(select(func.now()))).scalar_one()
        schedules = (
            await session.execute(
                select(
                    Schedule.id,
                    Schedule.pipeline_id,
                    Pipeline.tenant_id,
                    Schedule.cron_expression,
                    Schedule.timezone,
                    Schedule.handled_until,
                )
                .join(Pipeline, Schedule.pipeline_id == Pipeline.id)
                .where(Schedule.is_active.is_(True))
                .order_by(Schedule.id)
            )
        ).all()

    created = 0
    for schedule_id, pipeline_id, tenant_id, cron_expression, time_zone, handled_until in schedules:
        # one schedule that is broken or fails must not stop the others
        try:
            if handled_until is None:
                # a schedule from before handled_until existed: start from now and do not guess about the past
                await _record_handled(schedule_id, now)
                continue
            fire_time = latest_fire_time(cron_expression, time_zone, handled_until, now)
            if fire_time is None:
                continue
            if await _start_run(schedule_id, pipeline_id, tenant_id, fire_time):
                created += 1
        except Exception as error:
            log.error("schedule_failed", schedule_id=schedule_id, pipeline_id=pipeline_id, error=str(error))
    return created


async def _start_run(schedule_id: int, pipeline_id: int, tenant_id: int, fire_time: datetime) -> bool:
    key = idempotency_key(schedule_id, fire_time)
    async with async_session() as session:
        run, created = await create_pipeline_run_service(tenant_id, pipeline_id, session, key)
    if run is None:
        return False  # the pipeline was deleted after the list was read
    # recorded after the run exists, so a stop in between makes the next sweep find the run, not lose the fire time
    await _record_handled(schedule_id, fire_time)
    if created:
        log.info("scheduled_run_created", schedule_id=schedule_id, pipeline_id=pipeline_id, run_id=run.id, fire_time=fire_time.isoformat())
        metrics.scheduled_runs_total.inc()
        await enqueue_run(run.id)
    return created


async def _record_handled(schedule_id: int, handled_until: datetime) -> None:
    async with async_session() as session:
        await session.execute(
            update(Schedule)
            .where(Schedule.id == schedule_id)
            # greatest(), so a slow sweep never moves the mark back past a change the control plane just made
            .values(handled_until=func.greatest(func.coalesce(Schedule.handled_until, handled_until), handled_until))
        )
        await session.commit()


async def scheduler_loop(stop: asyncio.Event | None = None) -> None:
    """Check again and again. A failed check is logged and tried again, it never ends the loop."""
    while stop is None or not stop.is_set():
        try:
            await fire_due_schedules()
        except Exception as error:
            log.error("scheduler_sweep_failed", error=str(error))
        if stop is None:
            await asyncio.sleep(SCHEDULER_INTERVAL_SECONDS)
        else:
            try:
                await asyncio.wait_for(stop.wait(), timeout=SCHEDULER_INTERVAL_SECONDS)
            except asyncio.TimeoutError:
                pass
