"""Incident indexer, called fire-and-forget from run_diagnostic_agent after a recommendation is persisted.

Indexes ONE failure event into two rows in incident_embeddings: one for the run failure itself (source_type='past_run'), one for the recommendation (source_type='past_recommendation'). Both are tenant-scoped.

The function must be defensive. It runs as a background task, and any exception here would surface as an unhandled task exception, polluting logs without affecting the user-visible response.
We catch everything and log it, never raise.
"""

from shared.db import async_session
from shared.embeddings import embed_texts_async
from shared.observability import get_logger
from sqlalchemy.exc import SQLAlchemyError
from control_plane.app.models.incident_embeddings import IncidentEmbedding

log=get_logger(__name__)


def _summarize_attempt_pattern(step_attempts: list[dict]) -> str:
    """Deterministically describe the attempt pattern for a failed run.

    This is the same kind of signal log_analysis_node generates, but here we want determinism since we're constructing embedding text, not asking an LLM to interpret. The synthesized text
    directly affects what the embedding represents, so it needs to stay stable across identical failures, letting semantically-similar past failures cluster together in vector space.

    Heuristics:
      - 0 attempts: 'no attempts recorded' (defensive, shouldn't happen if the run actually failed)
      - 1 attempt: 'failed on first attempt with no retries configured'
      - N>1 attempts all with same error: 'failed N times with the same error after retries'
      - N>1 attempts with varying errors: 'failed N times with varying errors after retries'
    """
    if not step_attempts:
        return "no attempts recorded"
    
    #only FAILED attempts matter for pattern description; successful attempts mid-run aren't the failure signal.
    failed=[a for a in step_attempts if a.get("status")=="failed"]
    if not failed:
        return "no failed attempts recorded"

    n=len(failed)
    if n==1:
        return "failed on first attempt with no retries configured"

    #if all failures had the same error string, the failure is systematic (deterministic bug, dead URL, etc.).
    #if they varied, retries hit different errors, which suggests transient infrastructure issues.
    errors={a.get("error") for a in failed}
    if len(errors)==1:
        return f"failed {n} times with the same error after retries"
    return f"failed {n} times with varying errors after retries"


def _build_run_failure_text(run_context: dict) -> str:
    """Synthesize the embedding text for a past_run row.

    Raw error strings embed poorly because they're too thin: "ConnectionTimeout" alone embeds as a generic timeout concept.
    Synthesizing structured prose around the error gives the embedding model strong signal about what kind of failure this was, which is what retrieval needs.
    """
    error_type=run_context.get("error_type", "Unknown")
    error_message=run_context.get("error_message", "No error message available")
    step_attempts=run_context.get("step_attempts", [])

    #failed_step is the step_type of the LAST failed attempt, the step that ultimately caused the run to fail.
    failed_step="unknown"
    for attempt in reversed(step_attempts):
        if attempt.get("status")=="failed":
            failed_step=attempt.get("step_type", "unknown")
            break

    attempt_pattern=_summarize_attempt_pattern(step_attempts)

    return (f"Pipeline failure: {error_type} during {failed_step} step.\n"
            f"Error message: {error_message}\nAttempt pattern: {attempt_pattern}")


def _build_recommendation_text(failure_classification: str,recommended_action: str,explanation: str) -> str:
    """Synthesize the embedding text for a past_recommendation row.

    We embed the full triple (classification, action, explanation) so the embedding captures the full reasoning of the recommendation. The explanation field is the longest input and
    contributes the most signal, since it's where the agent grounds its reasoning in specific evidence.
    """
    return (f"Failure classification: {failure_classification}\n"
            f"Recommended action: {recommended_action}\n"
            f"Explanation: {explanation}")


def _extract_failed_step(run_context: dict) -> str:
    """Same logic as inside _build_run_failure_text, pulled out so we can put failed_step in meta without duplicating the synthesis call."""
    for attempt in reversed(run_context.get("step_attempts", [])):
        if attempt.get("status")=="failed":
            return attempt.get("step_type", "unknown")
    return "unknown"


async def index_incident(run_context: dict,recommendation_id: int,failure_classification: str,recommended_action: str,explanation: str,) -> None:
    """Index one failure event into incident_embeddings.

    Writes two rows in a single transaction: past_run + past_recommendation. Both are tenant-scoped via tenant_id from run_context, which keeps retrieval from leaking across tenants.

    This is fire-and-forget from the caller's perspective. Any failure is caught and logged; no exception propagates. The agent's recommendation_id was already persisted before this runs,
    so failing to index doesn't affect the user-visible response, it just means this particular incident won't show up in future retrievals.

    Parameters are passed individually (rather than fetching the recommendation row from DB) so we don't need to round-trip to the DB just to read fields we already have in memory at the call site.
    """
    try:
        run_id=run_context.get("run_id")
        pipeline_id=run_context.get("pipeline_id")
        tenant_id=run_context.get("tenant_id")
        error_type=run_context.get("error_type", "Unknown")

        #defensive: if any of these are missing, we can't index. Bail out with a log.
        #this should never happen if the executor populated run_context properly, but better to log and move on than blow up the background task.
        if run_id is None or pipeline_id is None or tenant_id is None:
            log.warning("incident_index_skipped_missing_ids", run_id=run_id, pipeline_id=pipeline_id, tenant_id=tenant_id)
            return

        failed_step=_extract_failed_step(run_context)
        run_failure_text=_build_run_failure_text(run_context)
        recommendation_text=_build_recommendation_text(failure_classification, recommended_action, explanation)

        #batch-embed both texts in one API call. Saves a round-trip vs. embedding each separately.
        embeddings=await embed_texts_async([run_failure_text, recommendation_text])
        run_embedding, rec_embedding = embeddings[0], embeddings[1]

        #fresh async session, does not inherit the executor's session. Same pattern as the webhook dispatcher and the agent itself.
        #keeps indexing concerns fully isolated from the main execution path.
        async with async_session() as session:
            try:
                run_row=IncidentEmbedding(source_type="past_run",
                    source_id=run_id, #FK semantics enforced in code: source_id points to pipeline_runs.id when source_type='past_run'
                    source_ref=f"run_id={run_id}",chunk_text=run_failure_text,embedding=run_embedding,tenant_id=tenant_id,
                    meta={"pipeline_id": pipeline_id,"error_type": error_type,"failed_step": failed_step,},)

                rec_row=IncidentEmbedding(source_type="past_recommendation",
                    source_id=recommendation_id, #points to agent_recommendations.id
                    source_ref=f"rec_id={recommendation_id}",chunk_text=recommendation_text,embedding=rec_embedding,tenant_id=tenant_id,
                    meta={"pipeline_id": pipeline_id,"failure_classification": failure_classification, "recommended_action": recommended_action,
                        #status is always 'pending' at index time; the tenant hasn't decided whether to apply or dismiss yet.
                        #later, when update_agent_recommendation_service flips status to applied/dismissed, that hook will need to
                        #update meta->>'status' here. Known TODO, not built yet: retrieval falls back to similarity-only ranking until then.
                        "status": "pending",},)

                session.add(run_row)
                session.add(rec_row)
                await session.commit()
                log.info("incident_indexed", run_id=run_id, recommendation_id=recommendation_id)

            except SQLAlchemyError as e:
                await session.rollback()
                log.error("incident_index_db_error", run_id=run_id, error=str(e))

    except Exception as e:
        #broad catch is intentional. This runs as a background task; an uncaught exception here becomes a task-level error that pollutes logs without affecting the response.
        log.error("incident_index_failed", error=str(e))