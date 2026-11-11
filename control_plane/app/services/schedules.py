from control_plane.app.models.pipelines import Pipeline
from control_plane.app.models.schedules import Schedule
from control_plane.app.schemas.schedules import ScheduleCreate, ScheduleUpdate
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import delete, func, select
from control_plane.app.exceptions import DuplicateScheduleError

async def create_schedule_service(tenant_id: int, pipeline_id: int, schedule_data: ScheduleCreate, session: AsyncSession):
    #we need to verify the pipeline exists and belongs to this tenant
    result=await session.execute(select(Pipeline).where(Pipeline.id == pipeline_id, Pipeline.tenant_id == tenant_id))
    pipeline=result.scalar_one_or_none()
    if not pipeline:
        return None  # route will handle 404 not found

    # check if schedule already exists for this pipeline
    existing_schedule=await session.execute(select(Schedule).where(Schedule.pipeline_id==pipeline_id))
    existing_schedule=existing_schedule.scalar_one_or_none()
    if existing_schedule:
        raise DuplicateScheduleError("A schedule already exists for this pipeline.")


    schedule_dict=schedule_data.model_dump()
    schedule_dict["pipeline_id"]=pipeline_id  #inject pipeline_id from the URL, not the body
    #a new schedule runs from its next fire time on. It does not start runs for times before it existed
    schedule_dict["handled_until"]=func.now()
    schedule=Schedule(**schedule_dict)

    try:
        session.add(schedule)
        await session.commit()
        await session.refresh(schedule)
        return schedule
    except SQLAlchemyError:
        await session.rollback()
        raise

async def get_schedule_service(tenant_id: int, pipeline_id: int, session: AsyncSession):
    #the schedule must exist, belong to the given pipeline, and that pipeline must belong to the tenant
    #we join through Pipeline to verify tenant ownership without a separate query
    result=await session.execute(
        select(Schedule).join(Pipeline,Schedule.pipeline_id==Pipeline.id).where(Schedule.pipeline_id==pipeline_id,Pipeline.tenant_id==tenant_id)
        )
    schedule=result.scalar_one_or_none()
    return schedule

async def delete_schedule_service(tenant_id: int, pipeline_id: int, session: AsyncSession):
    #we join through Pipeline to confirm the tenant owns this pipeline
    try:
        #first confirm the pipeline belongs to this tenant
        pipeline_result=await session.execute(select(Pipeline).where(Pipeline.id == pipeline_id, Pipeline.tenant_id == tenant_id))
        if not pipeline_result.scalar_one_or_none():
            return 0 #route treats 0 deleted rows as 404

        result=await session.execute(delete(Schedule).where(Schedule.pipeline_id == pipeline_id))
        await session.commit()
        return result.rowcount
    except SQLAlchemyError:
        await session.rollback()
        raise

async def update_schedule_service(tenant_id: int, pipeline_id: int, schedule_data: ScheduleUpdate, session: AsyncSession):
    #again verify schedule exists under this pipeline under this tenant
    result=await session.execute(select(Schedule).join(Pipeline, Schedule.pipeline_id == Pipeline.id).where(Schedule.pipeline_id==pipeline_id,Pipeline.tenant_id==tenant_id))
    schedule=result.scalar_one_or_none()
    if not schedule:
        return None

    update_dict=schedule_data.model_dump(exclude_unset=True)
    if not update_dict:
        raise ValueError("No fields were provided for update.")

    try:
        #a schedule that changes its timing, or is switched back on, runs from its next fire time on. Without this a
        #schedule paused for a week would start a run the moment it is switched on, for a time it was paused through
        restarts=(
            any(getattr(schedule, key)!=update_dict[key] for key in ("cron_expression","timezone") if key in update_dict)
            or (update_dict.get("is_active") is True and not schedule.is_active)
        )
        for key, value in update_dict.items():
            setattr(schedule, key, value)
        if restarts:
            schedule.handled_until=func.now()
        await session.commit()
        await session.refresh(schedule)
        return schedule
    except SQLAlchemyError:
        await session.rollback()
        raise