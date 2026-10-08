from pydantic import BaseModel
from datetime import datetime
from typing import Optional, Literal

StatusType=Literal["pending", "applied", "dismissed"]

class AgentRecommendationResponse(BaseModel):

    id: int
    tenant_id: int
    pipeline_id: int
    run_id: int
    failure_classification: str
    recommended_action: str
    explanation: str
    status: StatusType
    created_at: datetime
    updated_at: Optional[datetime]
    # what happened when a person applied it, for example "queued run 42". None on every other read.
    applied_effect: Optional[str] = None

    model_config = {
        "from_attributes": True
    }

class AgentRecommendationUpdate(BaseModel): 
    status: StatusType