from fastapi import APIRouter, Depends, status, HTTPException, Query 
from sqlalchemy.ext.asyncio import AsyncSession
from shared.db import get_db
from control_plane.app.services.agent_recommendations import get_agent_recommendation_service, get_all_agent_recommendations_service, get_all_agent_recommendations_for_pipeline_service, get_all_agent_recommendations_for_tenant_service, update_agent_recommendation_service
from control_plane.app.schemas.agent_recommendations import AgentRecommendationResponse, AgentRecommendationUpdate
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from typing import List
from shared.redis_client import redis_client
from shared.redis_client import redis_client

run_recommendations_router=APIRouter()
pipeline_recommendations_router=APIRouter()
tenant_recommendations_router=APIRouter()

#GET AGENT RECOMMENDATION BY ID
@run_recommendations_router.get("/{rec_id}", response_model=AgentRecommendationResponse)
async def get_agent_recommendation(tenant_id: int, pipeline_id: int, run_id: int, rec_id: int, session: AsyncSession=Depends(get_db)):

    rec_data=await get_agent_recommendation_service(tenant_id,pipeline_id,run_id,rec_id,session)
    if not rec_data:
        raise HTTPException(status_code=404,detail="Recommendation not found. Please check the rec_id")

    return rec_data

# GET ALL AGENT RECOMMENDATIONS FOR A PIPELINE RUN
@run_recommendations_router.get("/", response_model=List[AgentRecommendationResponse])
async def get_all_agent_recommendations(tenant_id: int, pipeline_id: int, run_id: int, session: AsyncSession=Depends(get_db), limit: int | None=Query(default=None,ge=1), offset: int=Query(default=0, ge=0)): #We user Query() to set constraints and for swagger docs. 
    #Limit must be greater than equal to 1 and offset greater than equal to 0 
    try:
        agent_recommendations=await get_all_agent_recommendations_service(tenant_id, pipeline_id, run_id, session, limit, offset)
    except SQLAlchemyError:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database error while fetching agent recommendations")
    
    if agent_recommendations is None: #needs 'is None' instead of 'not agent_recommendations' because None indicates pipeline_run was not found (atleast not for this pipeline, tenant or the tenant or pipeline doesn't exist), 
        # and an empty agent_recommendations list indicates 0 returned agent recommendations
        raise HTTPException(status_code=404,detail="Pipeline run not found. Please check the run_id")
    
    return agent_recommendations


# GET ALL AGENT RECOMMENDATIONS FOR A PIPELINE
@pipeline_recommendations_router.get("/", response_model=List[AgentRecommendationResponse])
async def get_all_agent_recommendations_for_pipeline(tenant_id: int, pipeline_id: int, session: AsyncSession=Depends(get_db), limit: int | None=Query(default=None,ge=1), offset: int=Query(default=0, ge=0)): #We user Query() to set constraints and for swagger docs. 
    #Limit must be greater than equal to 1 and offset greater than equal to 0 
    try:
        agent_recommendations=await get_all_agent_recommendations_for_pipeline_service(tenant_id, pipeline_id, session, limit, offset)
    except SQLAlchemyError:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database error while fetching agent recommendations")
    
    if agent_recommendations is None: #needs 'is None' instead of 'not agent_recommendations' because None indicates pipeline was not found (atleast not for this tenant, or the tenant doesn't exist),  
        # and an empty agent_recommendations list indicates 0 returned agent recommendations
        raise HTTPException(status_code=404,detail="Pipeline not found. Please check the pipeline_id")
    
    return agent_recommendations


# GET ALL AGENT RECOMMENDATIONS FOR A TENANT
@tenant_recommendations_router.get("/", response_model=List[AgentRecommendationResponse])
async def get_all_agent_recommendations_for_tenant(tenant_id: int, session: AsyncSession=Depends(get_db), limit: int | None=Query(default=None,ge=1), offset: int=Query(default=0, ge=0)): #We user Query() to set constraints and for swagger docs. 
    #Limit must be greater than equal to 1 and offset greater than equal to 0 
    try:
        agent_recommendations=await get_all_agent_recommendations_for_tenant_service(tenant_id, session, limit, offset)
    except SQLAlchemyError:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database error while fetching agent recommendations")
    
    if agent_recommendations is None: #needs 'is None' instead of 'not agent_recommendations' because None indicates tenant was not found, 
        # and an empty agent_recommendations list indicates 0 returned agent recommendations
        raise HTTPException(status_code=404,detail="Tenant not found. Please check the tenant_id")
    
    return agent_recommendations

# UPDATE AGENT RECOMMENDATION STATUS
@tenant_recommendations_router.put("/{rec_id}", response_model=AgentRecommendationResponse, status_code=status.HTTP_200_OK)
async def update_agent_recommendation(tenant_id: int, rec_id: int, rec_data: AgentRecommendationUpdate, session: AsyncSession=Depends(get_db)):
    try:
        agent_recommendation=await update_agent_recommendation_service(tenant_id,rec_id,rec_data,session)

    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,detail=str(e))
    except IntegrityError:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT,detail="Agent recommendation status could not be updated because of a database constraint violation.")
    except SQLAlchemyError:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail="Database error while updating agent recommendation.")
    
    if not agent_recommendation:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Agent recommendation not found. Please check the rec_id")

    #a retry created a run, so put it on the queue the worker reads
    if getattr(agent_recommendation, "enqueue_run_id", None) is not None:
        try:
            await redis_client.rpush("pipeline_runs", str(agent_recommendation.enqueue_run_id))
        except Exception:
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail="The recommendation was applied and a run was created, but it could not be queued. Start the run by hand.")
    
    return agent_recommendation