"""Keeps a run's lease alive while a worker works on it.

A claimed run has a lease: a time until which this worker owns it. If the worker stops, the lease runs out and the
reaper (worker/app/reaper.py) takes the run back. So a worker that is still alive has to keep pushing the lease
forward. That is all this does, as its own task beside the steps.

It can only extend the lease of a run that is still running or retrying. Once the run has ended, the UPDATE matches
nothing, so a heartbeat that fires late cannot bring a finished run back to life. A failed extension is logged and tried
at the next beat: a short database problem must not end a run that is going fine, and if it lasts a whole lease the
reaper is right to take the run.
"""
import asyncio
import contextlib
from datetime import timedelta

from sqlalchemy import func, update

from control_plane.app.models.pipeline_runs import PipelineRun
from shared.config import RUN_LEASE_SECONDS
from shared.db import async_session
from shared.observability import get_logger

log = get_logger(__name__)


async def extend_lease(run_id: int, lease_seconds: float = RUN_LEASE_SECONDS) -> bool:
    """Push the lease forward. Returns False when the run is no longer running, so there was nothing to extend."""
    async with async_session() as session:
        result = await session.execute(
            update(PipelineRun)
            .where(PipelineRun.id == run_id, PipelineRun.status.in_(("running", "retrying")))
            .values(lease_expires_at=func.now() + timedelta(seconds=lease_seconds))
        )
        await session.commit()
        return result.rowcount > 0


class LeaseKeeper:
    def __init__(self, run_id: int, lease_seconds: float = RUN_LEASE_SECONDS, beat_seconds: float | None = None):
        self.run_id = run_id
        self.lease_seconds = lease_seconds
        # three beats in a lease, so two in a row can fail before the lease is lost
        self.beat_seconds = beat_seconds if beat_seconds is not None else max(1.0, lease_seconds / 3)
        self._task: asyncio.Task | None = None

    def start(self) -> "LeaseKeeper":
        self._task = asyncio.create_task(self._beat())
        return self

    async def _beat(self) -> None:
        while True:
            await asyncio.sleep(self.beat_seconds)
            try:
                await extend_lease(self.run_id, self.lease_seconds)
            except Exception as error:
                log.warning("lease_extend_failed", run_id=self.run_id, error=str(error))

    async def stop(self) -> None:
        if self._task is None:
            return
        self._task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._task


def start_lease_keeper(run_id: int) -> LeaseKeeper:
    return LeaseKeeper(run_id).start()
