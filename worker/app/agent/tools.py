"""Tool-calling for the recovery-planning node: a callable the model can
invoke to check a pipeline's live circuit breaker state before recommending
retry_with_backoff or pause_schedule.

The recovery-planning prompt already has the classification and retrieved
past incidents, but neither tells it whether THIS pipeline's circuit breaker
is already open, or how close it is to tripping. That's live operational
state, which similarity search can't retrieve, so it needs a tool call
instead of more context stuffing.

Defensive posture matches every other LLM/DB touchpoint in this agent: a
DB error here returns a descriptive string rather than raising, so a broken
tool call degrades the recommendation's grounding, not the whole node.
"""
from langchain_core.tools import tool
from sqlalchemy import select

from control_plane.app.models.pipeline_circuit_breakers import PipelineCircuitBreaker
from shared.db import async_session


@tool
async def get_circuit_breaker_state(pipeline_id: int) -> str:
    """Look up the current circuit breaker state for a pipeline: whether it is
    open, closed, or half-open, why it tripped if it did, when it can next
    retry, and its failure threshold/window. Call this before recommending
    retry_with_backoff or pause_schedule for a network or quota failure --
    the circuit breaker may already be open (so pause_schedule is redundant)
    or close to tripping (so another retry_with_backoff could push it open).
    Do not call this for schema or unknown classifications; circuit breaker
    state does not change those recommendations.
    """
    try:
        async with async_session() as session:
            result = await session.execute(
                select(PipelineCircuitBreaker).where(PipelineCircuitBreaker.pipeline_id == pipeline_id)
            )
            cb = result.scalar_one_or_none()
    except Exception as e:
        return f"Could not check circuit breaker state for pipeline_id={pipeline_id}: {e}"

    if cb is None:
        return f"No circuit breaker record exists yet for pipeline_id={pipeline_id} (no prior failures recorded for it)."

    parts = [
        f"state={cb.state}",
        f"failure_count_threshold={cb.failure_count_threshold}",
        f"failure_window_minutes={cb.failure_window_minutes}",
    ]
    if cb.failure_reason:
        parts.append(f"failure_reason={cb.failure_reason}")
    if cb.retry_after:
        parts.append(f"retry_after={cb.retry_after.isoformat()}")
    return f"Circuit breaker for pipeline_id={pipeline_id}: " + ", ".join(parts)
