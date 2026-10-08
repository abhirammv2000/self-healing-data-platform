from control_plane.app.models.tenants import Tenant
from control_plane.app.models.pipelines import Pipeline
from control_plane.app.models.pipeline_runs import PipelineRun
from control_plane.app.models.agent_recommendations import AgentRecommendation
from control_plane.app.models.schedules import Schedule
from control_plane.app.services.pipeline_runs import create_pipeline_run_service
from control_plane.app.schemas.agent_recommendations import AgentRecommendationUpdate
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime

async def get_agent_recommendation_service(tenant_id: int, pipeline_id: int, run_id: int, rec_id: int, session: AsyncSession):
    #the recommendation must exist, belong to the given pipeline, and that pipeline must belong to the tenant
    #we join through PipelineRun to verify pipeline and tenant ownership without a separate query
    result=await session.execute(
        select(AgentRecommendation).join(PipelineRun,AgentRecommendation.run_id==PipelineRun.id)
        .where(AgentRecommendation.id==rec_id,AgentRecommendation.run_id==run_id,PipelineRun.tenant_id==tenant_id, PipelineRun.pipeline_id==pipeline_id))
    agent_recommendation=result.scalar_one_or_none()
    return agent_recommendation

async def get_all_agent_recommendations_service(tenant_id: int, pipeline_id: int, run_id: int, session: AsyncSession, limit: int | None = None, offset: int = 0):
    #we verify the run exists and belongs to this tenant and pipeline
    result=await session.execute(select(PipelineRun).where(PipelineRun.id==run_id,PipelineRun.tenant_id==tenant_id,PipelineRun.pipeline_id==pipeline_id))
    pipeline_run=result.scalar_one_or_none()
    if not pipeline_run:
        return None #route will handle 404

    statement=select(AgentRecommendation).where(AgentRecommendation.run_id==run_id).order_by(AgentRecommendation.created_at.desc()).offset(offset)
    if limit is not None:
        statement=statement.limit(limit)

    result=await session.execute(statement)
    agent_recommendations=result.scalars().all()
    return agent_recommendations

async def get_all_agent_recommendations_for_pipeline_service(tenant_id: int, pipeline_id: int, session: AsyncSession, limit: int | None = None, offset: int = 0):
    #we verify the pipeline exists and belongs to this tenant
    result=await session.execute(select(Pipeline).where(Pipeline.id==pipeline_id,Pipeline.tenant_id==tenant_id))
    pipeline=result.scalar_one_or_none()
    if not pipeline:
        return None #route will handle 404

    statement=select(AgentRecommendation).where(AgentRecommendation.pipeline_id==pipeline_id).order_by(AgentRecommendation.created_at.desc()).offset(offset)
    if limit is not None:
        statement=statement.limit(limit)

    result=await session.execute(statement)
    agent_recommendations=result.scalars().all()
    return agent_recommendations

async def get_all_agent_recommendations_for_tenant_service(tenant_id: int, session: AsyncSession, limit: int | None = None, offset: int = 0):
    #we verify the tenant exists
    result=await session.execute(select(Tenant).where(Tenant.id==tenant_id))
    tenant=result.scalar_one_or_none()
    if not tenant:
        return None #route will handle 404

    statement=select(AgentRecommendation).where(AgentRecommendation.tenant_id==tenant_id).order_by(AgentRecommendation.created_at.desc()).offset(offset)
    if limit is not None:
        statement=statement.limit(limit)

    result=await session.execute(statement)
    agent_recommendations=result.scalars().all()
    return agent_recommendations

async def _apply_recommendation(rec, session: AsyncSession) -> str:
    """Do what the agent recommended, for the two actions that are safe to do automatically once a person
    has approved. Returns a sentence saying what happened. The caller enqueues rec.enqueue_run_id if it is set.

    retry and retry_with_backoff start a new run of the same pipeline. The run's idempotency key is built from
    the recommendation id, so approving the same recommendation twice can never start two runs.
    pause_schedule turns the pipeline's schedule off.
    Everything else (schema_evolution, replay_from_raw, escalate) changes data or needs judgement, so approving
    it only records the decision and the person does the work.
    """
    action=rec.recommended_action
    if action in ("retry", "retry_with_backoff"):
        run, created=await create_pipeline_run_service(rec.tenant_id, rec.pipeline_id, session, idempotency_key=f"recommendation-{rec.id}")
        if run is None:
            raise ValueError("the pipeline this recommendation is for no longer exists")
        if created:
            rec.enqueue_run_id=run.id
            return f"started run {run.id}"
        return f"run {run.id} was already started for this recommendation"
    if action=="pause_schedule":
        result=await session.execute(select(Schedule).where(Schedule.pipeline_id==rec.pipeline_id))
        schedule=result.scalar_one_or_none()
        if schedule is None:
            return "the pipeline has no schedule, so there was nothing to pause"
        schedule.is_active=False
        return "paused the schedule"
    return f"recorded only. '{action}' is not done automatically, so do it by hand"

async def update_agent_recommendation_service(tenant_id: int, rec_id: int, rec_data: AgentRecommendationUpdate, session: AsyncSession):
    #verify recommendation exists under under this tenant
    result=await session.execute(select(AgentRecommendation).where(AgentRecommendation.id==rec_id, AgentRecommendation.tenant_id==tenant_id))
    agent_recommendation=result.scalar_one_or_none()
    if not agent_recommendation:
        return None #route will handle 404

    #a person decides once. Applying twice would repeat the action and an applied recommendation cannot be dismissed
    if agent_recommendation.status!="pending":
        raise ValueError(f"this recommendation is already {agent_recommendation.status}")
    if rec_data.status=="pending":
        raise ValueError("a recommendation cannot be moved back to pending")

    # if not rec_data or not rec_data.status: #this will never trigger as pydantic already checks if the status is provided
    #     raise ValueError("No 'status' was provided for update.")

    try:
        #the action runs first and the status changes in the same commit, so a failed action leaves it pending
        applied_effect=None
        if rec_data.status=="applied":
            applied_effect=await _apply_recommendation(agent_recommendation, session)
        enqueue_run_id=getattr(agent_recommendation, "enqueue_run_id", None)
        agent_recommendation.status=rec_data.status
        agent_recommendation.updated_at=datetime.now().replace(tzinfo=None)

        await session.commit()
        await session.refresh(agent_recommendation)
        agent_recommendation.applied_effect=applied_effect
        agent_recommendation.enqueue_run_id=enqueue_run_id
        return agent_recommendation
    except SQLAlchemyError:
        await session.rollback()
        raise
