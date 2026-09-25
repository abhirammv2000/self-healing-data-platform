import httpx
from shared.utils import now_naive
from shared.db import async_session
from shared.observability import get_logger
from control_plane.app.models.webhook_callbacks import WebhookCallback
from sqlalchemy.exc import SQLAlchemyError

log=get_logger(__name__)

async def dispatch_webhook_callback(callback_url: str, run_id: int, pipeline_id: int, tenant_id: int, status: str, recommendation_id: int|None=None):
    #URL is path-only (no scheme://host); the tenant's receiver resolves it
    #against their configured control plane base URL. Hardcoding localhost
    #here would leak into production, so base URL injection is deferred to
    #deployment config (CONTROL_PLANE_BASE_URL, once we're on GCP).
    recommendations_url=None
    if recommendation_id is not None:
        recommendations_url=f"/tenants/{tenant_id}/pipelines/{pipeline_id}/runs/{run_id}/recommendations/{recommendation_id}"
    
    payload={"run_id":run_id, "pipeline_id": pipeline_id, "tenant_id":tenant_id, "status":status, "timestamp":now_naive().isoformat(), "recommendations_url":recommendations_url}
    payload["event"]="run.completed" if status=="success" else "run.failed"

    webhook_callback_dict={"tenant_id": tenant_id, "pipeline_id": pipeline_id, "run_id": run_id, "callback_url": callback_url, "payload": payload}
    
    try:
        async with httpx.AsyncClient(timeout=15) as client: #timeout avoids hanging if the callback endpoint never responds
            response=await client.post(url=callback_url,json=payload)

        webhook_callback_dict["http_status_code"]=response.status_code

        if 200<=response.status_code<300:
            webhook_callback_dict["status"]="success"
        else:
            webhook_callback_dict["status"]="failed"
            webhook_callback_dict["error_message"]=f"Webhook returned non-2xx status code: {response.status_code}"

    except Exception as e:
        webhook_callback_dict["status"]="failed"
        webhook_callback_dict["error_message"]=str(e)

    #write the delivery result to the audit table regardless of dispatch outcome
    async with async_session() as session:
        try:
            webhook_callback=WebhookCallback(**webhook_callback_dict)
            session.add(webhook_callback)
            await session.commit()
            await session.refresh(webhook_callback)

            return webhook_callback
        
        except SQLAlchemyError:
            await session.rollback()
            log.error("webhook_callback_write_failed", run_id=run_id)