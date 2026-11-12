"""Finds the runs that the queue or a worker lost, and gets them going again.

A run can be lost in two ways, and the queue alone cannot recover from either:

  * It was saved but never reached the queue, or its id was taken off the queue by a worker that stopped before it
    claimed the run. The run is 'queued' and nothing will ever start it. requeue_unqueued_runs() puts its id on the
    queue again.
  * A worker claimed the run and then stopped (crashed, killed, lost its machine). The run is 'running' for ever.
    The worker extends a lease while it works, so a lease in the past means the worker is gone.
    recover_dead_runs() puts such a run back to 'queued', or fails it after RUN_MAX_ATTEMPTS claims so a run that
    keeps killing its worker does not loop.

Putting an id on the queue twice is safe, because a worker claims a run with one conditional UPDATE and only one
claim can win. Every worker runs this loop, and the rows are locked with SKIP LOCKED, so two reapers never take the
same run. All times are compared with the database's own now(), never with a worker's clock.

A run that is taken over starts again from its first step. That is safe only because steps can be repeated: ingestion
writes a new file and the load step replaces its table. A new kind of step has to keep that true.
"""
import asyncio
from datetime import timedelta

from sqlalchemy import func, or_, select, update

from control_plane.app.models.pipeline_runs import PipelineRun
from shared import metrics
from shared.config import ENQUEUE_VISIBILITY_SECONDS, REAPER_INTERVAL_SECONDS, RUN_MAX_ATTEMPTS
from shared.db import async_session
from shared.observability import get_logger
from shared.redis_client import redis_client
from shared.run_queue import QUEUE_NAME
from shared.utils import now_naive

log = get_logger(__name__)

# the most runs one sweep takes, so a large backlog is worked through over several sweeps and not in one long lock
BATCH = 100


async def recover_dead_runs(max_attempts: int = RUN_MAX_ATTEMPTS) -> tuple[int, int]:
    """Deal with runs whose worker stopped. Returns (put back on the queue, failed)."""
    async with async_session() as session:
        rows = (
            await session.execute(
                select(PipelineRun.id, PipelineRun.attempts)
                .where(PipelineRun.status.in_(("running", "retrying")), PipelineRun.lease_expires_at < func.now())
                .order_by(PipelineRun.id)
                .limit(BATCH)
                .with_for_update(skip_locked=True)
            )
        ).all()
        give_up = [run_id for run_id, attempts in rows if attempts >= max_attempts]
        try_again = [run_id for run_id, attempts in rows if attempts < max_attempts]
        now = now_naive()

        if give_up:
            await session.execute(
                update(PipelineRun)
                .where(PipelineRun.id.in_(give_up))
                .values(
                    status="failed",
                    error_type="worker_lost",
                    error_message=f"The worker running this stopped {max_attempts} times before it finished.",
                    ended_at=now,
                    updated_at=now,
                    lease_expires_at=None,
                )
            )
        if try_again:
            await session.execute(
                update(PipelineRun)
                .where(PipelineRun.id.in_(try_again))
                .values(status="queued", lease_expires_at=None, last_enqueued_at=None, updated_at=now)
            )
        await session.commit()

    for run_id in give_up:
        log.error("run_failed_worker_lost", run_id=run_id, max_attempts=max_attempts)
    for run_id in try_again:
        log.warning("run_requeued_worker_lost", run_id=run_id)
    if give_up:
        metrics.pipeline_runs_recovered_total.labels(reason="failed_worker_lost").inc(len(give_up))
        metrics.pipeline_runs_total.labels(status="failed").inc(len(give_up))
    if try_again:
        metrics.pipeline_runs_recovered_total.labels(reason="requeued_worker_lost").inc(len(try_again))
    return len(try_again), len(give_up)


async def requeue_unqueued_runs() -> int:
    """Put the id of every queued run that is not on the queue on it. Returns how many were pushed."""
    async with async_session() as session:
        run_ids = (
            (
                await session.execute(
                    select(PipelineRun.id)
                    .where(
                        PipelineRun.status == "queued",
                        or_(
                            PipelineRun.last_enqueued_at.is_(None),
                            PipelineRun.last_enqueued_at < func.now() - timedelta(seconds=ENQUEUE_VISIBILITY_SECONDS),
                        ),
                    )
                    .order_by(PipelineRun.id)
                    .limit(BATCH)
                    .with_for_update(skip_locked=True)
                )
            )
            .scalars()
            .all()
        )
        if not run_ids:
            return 0
        # marked before the push, in the same transaction that holds the locks, so another reaper cannot also take them
        await session.execute(update(PipelineRun).where(PipelineRun.id.in_(run_ids)).values(last_enqueued_at=func.now()))
        await session.commit()

    pushed = 0
    try:
        for run_id in run_ids:
            await redis_client.rpush(QUEUE_NAME, str(run_id))
            pushed += 1
    except Exception as error:
        log.warning("requeue_failed", pushed=pushed, waiting=len(run_ids) - pushed, error=str(error))
        # the ones that did not get there are marked as not enqueued, so the next sweep tries them again
        async with async_session() as session:
            await session.execute(
                update(PipelineRun)
                .where(PipelineRun.id.in_(run_ids[pushed:]), PipelineRun.status == "queued")
                .values(last_enqueued_at=None)
            )
            await session.commit()

    if pushed:
        log.info("runs_queued_again", count=pushed)
        metrics.pipeline_runs_recovered_total.labels(reason="queued_again").inc(pushed)
    return pushed


async def reaper_loop(stop: asyncio.Event | None = None) -> None:
    """Sweep again and again. A failed sweep is logged and tried again, it never ends the loop."""
    while stop is None or not stop.is_set():
        try:
            await recover_dead_runs()
            await requeue_unqueued_runs()
        except Exception as error:
            log.error("reaper_sweep_failed", error=str(error))
        if stop is None:
            await asyncio.sleep(REAPER_INTERVAL_SECONDS)
        else:
            try:
                await asyncio.wait_for(stop.wait(), timeout=REAPER_INTERVAL_SECONDS)
            except asyncio.TimeoutError:
                pass
