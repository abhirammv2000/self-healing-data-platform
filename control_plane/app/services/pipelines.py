from control_plane.app.models.tenants import Tenant
from control_plane.app.models.pipelines import Pipeline
from control_plane.app.models.pipeline_circuit_breakers import PipelineCircuitBreaker
from control_plane.app.schemas.pipelines import PipelineCreate, PipelineUpdate
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import delete, select

async def create_pipeline_service(tenant_id: int, pipeline_data: PipelineCreate, session: AsyncSession):
    
    tenant=await session.get(Tenant,tenant_id) #first we get the Tenant object
    if not tenant: #and check if the tenant exists or not
        return None #route will handle 404 tenant not found

    pipeline_dict=pipeline_data.model_dump() #converts the pydantic object to a plain dictionary
    pipeline_dict["tenant_id"]=tenant_id
    pipeline=Pipeline(**pipeline_dict) #SQLAlchemy constructors expect keyword arguments, so we need to unpack the dictionary with **

    try:
        #first we try to create the pipeline
        session.add(pipeline) #this makes SQL track and prepare the object to insert it into the database
        await session.flush() #flush sends the Pipeline to the DB to get an ID WITHOUT committing the transaction yet

        #whenever a pipeline is created, we also create a circuit breaker record for it. The circuit state is defaulted to 'closed'.
        pipeline_circuit_breaker_dict={"pipeline_id":pipeline.id,"tenant_id":pipeline.tenant_id}

        pipeline_circuit_breaker=PipelineCircuitBreaker(**pipeline_circuit_breaker_dict)
        session.add(pipeline_circuit_breaker)

        await session.commit() #commit the whole session only after both operations run successfully
        await session.refresh(pipeline) #because the DB generates certain fields, we wait for it to send the latest state to be reflected in the pipeline object.

        return pipeline #we return this full object so we can send it in the Response later
    except SQLAlchemyError:
        await session.rollback() #incase of any DB related errors, we roll back the session and raise the error
        raise

async def get_pipeline_service(tenant_id: int, pipeline_id: int, session: AsyncSession):
    #we check both the pipeline id as well as the tenant id to ensure only the correct tenant can get the pipeline
    #If pipeline doesn't exist, then returns none. If pipeline exists but belongs to another tenant, also returns None. This ensures we dont leak information like "this pipeline exists but not yours"
    result=await session.execute(select(Pipeline).where(Pipeline.id==pipeline_id,Pipeline.tenant_id==tenant_id))
    pipeline_data=result.scalar_one_or_none()
    return pipeline_data

async def get_all_pipelines_service(tenant_id: int, session:AsyncSession, limit: int | None=None, offset: int=0): 

    tenant=await session.get(Tenant, tenant_id) #first we check if this tenant exists
    if not tenant:
        return None  #route will handle 404 tenant not found
    
    statement=select(Pipeline).where(Pipeline.tenant_id==tenant_id).order_by(Pipeline.id).offset(offset) #we select all pipelines of a specific tenant with optional offset (0 by default). we also ensure they are sorted by id
    if limit is not None: #and an optional limit. None by default
        statement=statement.limit(limit)

    result=await session.execute(statement)
    pipelines=result.scalars().all() #scalars() is used to get clean ORM objects. all() is used to give the results as a list
    return pipelines

async def delete_pipeline_service(tenant_id: int, pipeline_id: int, session: AsyncSession):
    
    try:
        result=await session.execute(delete(Pipeline).where(Pipeline.tenant_id==tenant_id,Pipeline.id==pipeline_id)) #try deleting this pipeline id if it belongs to the tenant_id and then return the number of rows deleted (should be 1 if deleted, 0 if not deleted)
        await session.commit()
        return result.rowcount
    except SQLAlchemyError: #if any DB related errors, then rollback
        await session.rollback() 
        raise


async def update_pipeline_service(tenant_id: int, pipeline_id: int, pipeline_data: PipelineUpdate, session: AsyncSession):
    
    result=await session.execute(select(Pipeline).where(Pipeline.id==pipeline_id,Pipeline.tenant_id==tenant_id)) #first we check if this pipeline exists for this specific tenant_id
    pipeline=result.scalar_one_or_none()
    if not pipeline:
        return None  #route will handle 404 pipeline not found
    
    update_dict=pipeline_data.model_dump(exclude_unset=True) #we get the dictionary of the updates. Exclude unset is set to True to avoid updating other fields
    if not update_dict:
        raise ValueError("No fields were provided for update.")
    
    try:
        for key,value in update_dict.items():
            setattr(pipeline,key,value) #then set the individual attributes

        await session.commit()
        await session.refresh(pipeline)

        return pipeline
    
    except SQLAlchemyError: #if any DB error, we rollback
        await session.rollback()
        raise