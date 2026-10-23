from fastapi import APIRouter, Depends, status, HTTPException 
from sqlalchemy.ext.asyncio import AsyncSession
from shared.db import get_db
from control_plane.app.services.schedules import create_schedule_service, get_schedule_service, delete_schedule_service, update_schedule_service
from control_plane.app.schemas.schedules import ScheduleCreate, ScheduleUpdate, ScheduleResponse
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from control_plane.app.exceptions import DuplicateScheduleError

router=APIRouter()

#CREATE SCHEDULE
@router.post("/", response_model=ScheduleResponse, status_code=status.HTTP_201_CREATED)
async def create_schedule(tenant_id: int, pipeline_id: int, schedule_data: ScheduleCreate, session: AsyncSession=Depends(get_db)):

    try:
        schedule=await create_schedule_service(tenant_id, pipeline_id, schedule_data, session)
    except DuplicateScheduleError as e:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT,detail=str(e))
    except IntegrityError:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT,detail="Pipeline schedule could not be created because of a database constraint violation.")
    except SQLAlchemyError:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail="Database error while creating pipeline schedule")
    
    if not schedule:
        raise HTTPException(status_code=404,detail="Pipeline not found. Please check the pipeline_id")

    return schedule

#GET SCHEDULE
@router.get("/", response_model=ScheduleResponse)
async def get_schedule(tenant_id: int, pipeline_id: int, session: AsyncSession=Depends(get_db)):

    schedule_data=await get_schedule_service(tenant_id,pipeline_id,session)
    if not schedule_data:
        raise HTTPException(status_code=404,detail="Pipeline schedule not found. Please check the pipeline_id")

    return schedule_data

#DELETE SCHEDULE FOR A SPECIFIC PIPELINE
@router.delete("/", status_code=status.HTTP_204_NO_CONTENT)
async def delete_schedule(tenant_id: int, pipeline_id: int, session: AsyncSession=Depends(get_db)):
    try:
        deleted_rows=await delete_schedule_service(tenant_id,pipeline_id,session)

    except SQLAlchemyError:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database error while deleting pipeline schedule")
    
    if deleted_rows==0:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Pipeline schedule not found. Please check the pipeline_id")
    return

#UPDATE SCHEDULE FOR A PIPELINE
@router.put("/", response_model=ScheduleResponse, status_code=status.HTTP_200_OK)
async def update_schedule(tenant_id: int, pipeline_id: int, schedule_data: ScheduleUpdate, session: AsyncSession=Depends(get_db)):
    try:
        schedule=await update_schedule_service(tenant_id,pipeline_id,schedule_data,session)

    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,detail=str(e))
    except IntegrityError:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT,detail="Pipeline schedule could not be updated because of a database constraint violation.")
    except SQLAlchemyError:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail="Database error while updating pipeline schedule.")
    
    
    if not schedule:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Pipeline schedule not found. Please check the pipeline_id")
    
    return schedule