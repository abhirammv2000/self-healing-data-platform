"""Putting a run on the queue the workers read.

A run is first saved in Postgres and only then put on the Redis queue. Those are two systems, so the second step can
fail after the first has worked, and the run would sit in the database with nothing to start it. This makes that safe:
the run is already durable, and a run that did not reach the queue is found by the reaper (worker/app/reaper.py) and
put there again. So a failed push is not an error for the caller. The run exists and will be started.
"""
from sqlalchemy import func, select, update

from control_plane.app.models.pipeline_runs import PipelineRun
from shared.db import async_session
from shared.observability import get_logger
from shared.redis_client import redis_client

log = get_logger(__name__)

QUEUE_NAME = "pipeline_runs"


async def enqueue_run(run_id: int) -> bool:
    """Put the run id on the queue and note when. Returns False when Redis was not reachable, never raises.

    False does not lose the run: its last_enqueued_at stays empty, and the reaper puts it on the queue.
    """
    try:
        await redis_client.rpush(QUEUE_NAME, str(run_id))
    except Exception as error:  # Redis can fail in many ways, and none of them may lose the run
        log.warning("enqueue_deferred", run_id=run_id, error=str(error))
        return False

    await mark_enqueued(run_id)
    return True


async def mark_enqueued(run_id: int) -> None:
    """Record that the id is on the queue now. If this fails, the reaper puts the id there again after a while, and
    claiming a run is atomic, so a second copy of the id does no harm."""
    try:
        async with async_session() as session:
            await session.execute(
                update(PipelineRun)
                .where(PipelineRun.id == run_id, PipelineRun.status == "queued")
                .values(last_enqueued_at=func.now())
            )
            await session.commit()
    except Exception as error:
        log.warning("enqueue_not_recorded", run_id=run_id, error=str(error))


async def wake_waiting_run(pipeline_id: int) -> bool:
    """A run of the pipeline just ended, so the pipeline is free: put the oldest queued run of it on the queue now.

    A pipeline runs one run at a time, and a run that finds its pipeline busy is left queued (worker/app/executor.py).
    Without this it would wait for the reaper to offer it again, up to ENQUEUE_VISIBILITY_SECONDS later, and a burst of
    runs for one pipeline would crawl. The id may already be on the queue, which is harmless: only one claim wins.
    Never raises. If it fails, the reaper offers the waiting run later.
    """
    try:
        async with async_session() as session:
            run_id = (
                await session.execute(
                    select(PipelineRun.id)
                    .where(PipelineRun.pipeline_id == pipeline_id, PipelineRun.status == "queued")
                    .order_by(PipelineRun.id)
                    .limit(1)
                )
            ).scalar_one_or_none()
    except Exception as error:
        log.warning("wake_waiting_run_failed", pipeline_id=pipeline_id, error=str(error))
        return False
    if run_id is None:
        return False
    return await enqueue_run(run_id)
