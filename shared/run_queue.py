"""Putting a run on the queue the workers read.

A run is first saved in Postgres and only then put on the Redis queue. Those are two systems, so the second step can
fail after the first has worked, and the run would sit in the database with nothing to start it. This makes that safe:
the run is already durable, and a run that did not reach the queue is found by the reaper (worker/app/reaper.py) and
put there again. So a failed push is not an error for the caller. The run exists and will be started.
"""
from sqlalchemy import func, update

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
