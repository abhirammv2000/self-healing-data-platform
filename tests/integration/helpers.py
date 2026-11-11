"""Helpers for the integration tests: build a tenant and pipelines with the project's own services, change a run's
state directly (to play a worker that died), and consume the queue the way worker/app/main.py does."""
import asyncio
import uuid

from sqlalchemy import select, text

from control_plane.app.models.pipeline_runs import PipelineRun
from control_plane.app.schemas.pipeline_steps import PipelineStepCreate
from control_plane.app.schemas.pipelines import PipelineCreate
from control_plane.app.schemas.tenants import TenantCreate
from control_plane.app.services.pipeline_runs import create_pipeline_run_service
from control_plane.app.services.pipeline_steps import create_pipeline_step_service
from control_plane.app.services.pipelines import create_pipeline_service
from control_plane.app.services.tenants import create_tenant_service
from shared.db import async_session
from shared.redis_client import redis_client
from shared.run_queue import QUEUE_NAME
from worker.app.exceptions import InvalidPipelineRunStatusError
from worker.app.executor import run_executor_service


class World:
    def __init__(self, source, tenant_id):
        self.source = source
        self.tenant_id = tenant_id

    @classmethod
    async def create(cls, source):
        async with async_session() as session:
            tenant, _key = await create_tenant_service(TenantCreate(name=f"it-{uuid.uuid4().hex[:8]}"), session)
        return cls(source, tenant.id)

    async def pipeline(self, path="/ok.csv", retries=0):
        """A pipeline with one ingestion step that reads `path` from the local data source."""
        async with async_session() as session:
            pipeline = await create_pipeline_service(
                self.tenant_id, PipelineCreate(name=f"p-{uuid.uuid4().hex[:6]}"), session
            )
            step = PipelineStepCreate(
                step_type="ingestion",
                step_order=1,
                config={"source_url": self.source + path, "retry": {"max_retries": retries, "base_delay": 0}},
            )
            await create_pipeline_step_service(self.tenant_id, pipeline.id, step, session)
        return pipeline.id

    async def run(self, pipeline_id, idempotency_key=None):
        """A new queued run that is NOT on the queue, as if the push to Redis had not happened yet."""
        async with async_session() as session:
            run, _created = await create_pipeline_run_service(self.tenant_id, pipeline_id, session, idempotency_key)
        return run.id


async def get_run(run_id):
    async with async_session() as session:
        return (await session.execute(select(PipelineRun).where(PipelineRun.id == run_id))).scalar_one()


async def set_run(run_id, **fields):
    """Write columns of a run directly. A value that is a string starting with 'sql:' is used as SQL, for example
    'sql:now() - interval \\'5 minutes\\''."""
    assignments, params = [], {"id": run_id}
    for name, value in fields.items():
        if isinstance(value, str) and value.startswith("sql:"):
            assignments.append(f"{name} = {value[4:]}")
        else:
            assignments.append(f"{name} = :{name}")
            params[name] = value
    async with async_session() as session:
        await session.execute(text(f"update pipeline_runs set {', '.join(assignments)} where id = :id"), params)
        await session.commit()


async def queue_contents():
    return [int(value) for value in await redis_client.lrange(QUEUE_NAME, 0, -1)]


async def work(workers=1, wait=1.0):
    """Consume the queue with `workers` concurrent loops, as that many workers would, until it has been empty for
    `wait` seconds. Returns how many times a worker found the run already taken, which is not a failure."""
    already_handled = 0

    async def one_worker():
        nonlocal already_handled
        while True:
            popped = await redis_client.blpop(QUEUE_NAME, timeout=wait)
            if popped is None:
                return
            try:
                await run_executor_service(int(popped[1]))
            except InvalidPipelineRunStatusError:
                already_handled += 1

    await asyncio.gather(*(one_worker() for _ in range(workers)))
    return already_handled
