from control_plane.app.models.tenants import Tenant
from control_plane.app.models.pipelines import Pipeline
from control_plane.app.models.pipeline_runs import PipelineRun
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError, SQLAlchemyError

async def get_pipeline_run_service(tenant_id: int, pipeline_id: int, run_id: int, session: AsyncSession):
    #the run must exist, belong to the given pipeline, and that pipeline must belong to the tenant
    #we join through Pipeline to verify tenant ownership without a separate query
    result=await session.execute(
        select(PipelineRun).join(Pipeline,PipelineRun.pipeline_id==Pipeline.id).where(PipelineRun.id==run_id,PipelineRun.pipeline_id==pipeline_id,Pipeline.tenant_id==tenant_id)
        )
    pipeline_run=result.scalar_one_or_none()
    return pipeline_run

async def get_all_pipeline_runs_service(tenant_id: int, pipeline_id: int, session: AsyncSession, limit: int | None = None, offset: int = 0):
    #we verify the pipeline exists and belongs to this tenant
    result=await session.execute(select(Pipeline).where(Pipeline.id==pipeline_id,Pipeline.tenant_id==tenant_id))
    pipeline=result.scalar_one_or_none()
    if not pipeline:
        return None #route will handle 404

    statement=select(PipelineRun).where(PipelineRun.pipeline_id==pipeline_id).order_by(PipelineRun.created_at.desc()).offset(offset)
    if limit is not None:
        statement=statement.limit(limit)

    result=await session.execute(statement)
    pipeline_runs=result.scalars().all()
    return pipeline_runs

async def get_all_pipeline_runs_for_tenant_service(tenant_id: int, session: AsyncSession, limit: int | None = None, offset: int = 0):
    #we verify the tenant exists
    result=await session.execute(select(Tenant).where(Tenant.id==tenant_id))
    tenant=result.scalar_one_or_none()
    if not tenant:
        return None #route will handle 404

    statement=select(PipelineRun).where(PipelineRun.tenant_id==tenant_id).order_by(PipelineRun.created_at.desc()).offset(offset)
    if limit is not None:
        statement=statement.limit(limit)

    result=await session.execute(statement)
    pipeline_runs=result.scalars().all()
    return pipeline_runs

async def create_pipeline_run_service(tenant_id: int, pipeline_id: int, session: AsyncSession, idempotency_key: str | None = None):
    """Returns (run, created) so the route can tell a fresh run (201, enqueue it) apart from
    a replayed one (200, don't enqueue it again).

    The DB's unique constraint on (pipeline_id, idempotency_key) is what actually enforces
    idempotency. The upfront SELECT is just a fast path for the common case (a client retry
    after a timed-out response); two concurrent requests with the same key could both pass it
    before either commits. The except IntegrityError block closes that race: whichever
    request's INSERT loses rolls back and re-reads the row the winner just created.
    """
    #we need to verify the pipeline exists and belongs to this tenant
    #without this, someone could create steps on a pipeline belonging to another tenant
    result=await session.execute(select(Pipeline).where(Pipeline.id == pipeline_id, Pipeline.tenant_id == tenant_id))
    pipeline = result.scalar_one_or_none()
    if not pipeline:
        return None, False  # route will handle 404 not found

    if idempotency_key is not None:
        result=await session.execute(select(PipelineRun).where(PipelineRun.pipeline_id == pipeline_id, PipelineRun.idempotency_key == idempotency_key))
        existing=result.scalar_one_or_none()
        if existing is not None:
            return existing, False

    run_dict={"tenant_id": tenant_id, "pipeline_id": pipeline_id, "idempotency_key": idempotency_key}
    run=PipelineRun(**run_dict)

    try:
        session.add(run)
        await session.commit()
        await session.refresh(run)
        return run, True
    except IntegrityError:
        #lost the race: another request with the same (pipeline_id, idempotency_key) committed
        #first. Roll back and hand back the row that won.
        await session.rollback()
        if idempotency_key is not None:
            result=await session.execute(select(PipelineRun).where(PipelineRun.pipeline_id == pipeline_id, PipelineRun.idempotency_key == idempotency_key))
            existing=result.scalar_one_or_none()
            if existing is not None:
                return existing, False
        raise  # constraint violation unrelated to idempotency, or the racing row vanished
    except SQLAlchemyError:
        await session.rollback()
        raise