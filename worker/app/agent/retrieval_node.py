"""Retrieval node, sits between classification and recovery_planning in the diagnostic graph.

Builds a structured query from upstream state (classification + log analysis + raw error metadata), embeds it, and runs two tenant-aware pgvector similarity searches against incident_embeddings:
  - Top K_RUNBOOKS chunks from runbooks (global, no tenant filter)
  - Top K_INCIDENTS chunks from past_run + past_recommendation rows (tenant-scoped)

The split-then-combine approach gives us diversity: pure top-5 by similarity could come back as all runbooks (more of them) or all incidents (tenant-scoped recency), neither of which gives Recovery Planning the breadth of context we want.

Failures here are absorbed: any exception falls back to an empty list, so downstream sees "no retrieved context", same as a legitimately empty result. Recovery Planning is written to handle the empty case explicitly.
"""

from sqlalchemy import text
from shared.db import async_session
from shared.embeddings import embed_text_async
from shared.observability import get_logger
from worker.app.agent.state import DiagnosticState, RetrievedChunk

log=get_logger(__name__)


# ============================================================================
# Tunable constants. Module-level so they're trivially adjustable without
# touching node logic. Current split (2 runbooks + 3 incidents = top 5) is a
# starting point; we can experiment later as the incident corpus grows.
# ============================================================================
K_RUNBOOKS=2 #how many runbook chunks to retrieve: operational knowledge, what SHOULD happen for this kind of failure.
K_INCIDENTS=3 #how many past_run + past_recommendation chunks to retrieve: tenant-specific history, what HAS happened for this tenant before.


def _build_query(state: DiagnosticState) -> str:
    """Synthesize the retrieval query string from upstream state.

    All fields are guaranteed populated by the time this node runs (classification has run, log_analysis has run, and both have sentinel fallbacks so they're never None at this point).
    Even in the sentinel/degraded case, we still get some signal from the raw error_type/error_message, which is the whole point of sentinel fallbacks instead of skipping retrieval entirely.

    The synthesized text matters more than expected: 'ConnectionTimeout' alone embeds as a generic timeout concept, but '{classification} failure during {failed_step}. Error: ConnectionTimeout - <message>' embeds as a
    specific failure-of-this-kind-at-this-step concept, which retrieves the right runbook section much more reliably.
    """
    classification=state["classification"]
    log_analysis=state["log_analysis"]
    error_type=state["error_type"]
    error_message=state["error_message"]

    #defensive .get-equivalent: if upstream nodes somehow returned None (graph wiring bug, not normal flow), still produce a workable query string from the raw error metadata.
    failure_classification=classification.failure_classification if classification else "unknown"
    failed_step=log_analysis.failed_step if log_analysis else "unknown"
    attempt_pattern=log_analysis.attempt_pattern if log_analysis else "unknown"

    return (f"{failure_classification} failure during {failed_step} step.\nError: {error_type} - {error_message}\nPattern: {attempt_pattern}")


# ============================================================================
# SQL queries, defined as constants up here so they're easy to read in one
# place. We use raw text() with parameterized binding rather than the ORM
# because pgvector's <=> operator isn't first-class in SQLAlchemy's ORM layer
# and the raw SQL is clearer for this kind of query.
#
# Note on CAST(:query_vec AS vector): this is how we get a Python list[float]
# into pgvector via SQLAlchemy. Direct binding requires the pgvector SQLAlchemy
# type system to be in the loop; the string-cast pattern works in any context
# and matches what Anirudh's earlier manual sanity check used successfully.
# ============================================================================

_RUNBOOK_QUERY=text("""
    SELECT source_type, source_ref, chunk_text,
           embedding <=> CAST(:query_vec AS vector) AS distance
    FROM incident_embeddings
    WHERE source_type = 'runbook'
    ORDER BY distance ASC
    LIMIT :k
""")

_INCIDENT_QUERY=text("""
    SELECT source_type, source_ref, chunk_text,
           embedding <=> CAST(:query_vec AS vector) AS distance
    FROM incident_embeddings
    WHERE source_type IN ('past_run', 'past_recommendation')
      AND tenant_id = :tenant_id
    ORDER BY distance ASC
    LIMIT :k
""")


async def retrieval_node(state: DiagnosticState) -> dict:
    """Retrieve operational and historical context for the Recovery Planning node.

    Reads: classification, log_analysis, error_type, error_message, tenant_id (from state)
    Writes: retrieved_context (a list of RetrievedChunk, possibly empty)
    """
    run_id=state["run_id"]
    tenant_id=state["tenant_id"]

    try:
        query=_build_query(state)

        #embed the query string using RETRIEVAL_QUERY task type (the embeddings helper handles this automatically via embed_query under the hood).
        #this is asymmetric retrieval: query and documents use different task types to maximize retrieval quality.
        query_vec=await embed_text_async(query)
        #pgvector's literal vector format is '[1.0,2.0,...]', which is exactly what str(list) gives us. Used in the CAST below.
        query_vec_str=str(query_vec)

        #run both queries in the same session. They're sequential here rather than parallel: gather() would shave milliseconds but adds complexity, and at our scale the sequential cost is invisible.
        async with async_session() as session:
            runbook_result=await session.execute(_RUNBOOK_QUERY,{"query_vec": query_vec_str, "k": K_RUNBOOKS})
            runbook_rows=runbook_result.all()

            incident_result=await session.execute(_INCIDENT_QUERY,{"query_vec": query_vec_str, "tenant_id": tenant_id, "k": K_INCIDENTS})
            incident_rows=incident_result.all()

        #combine and globally sort by distance ascending (most similar first). This interleaves runbooks and incidents: a tightly-matching runbook can outrank a loosely-matching past incident, and vice versa.
        #the alternative (always-runbooks-first or always-incidents-first) would force structural ordering that doesn't reflect actual relevance. Sorting globally lets similarity be the source of truth.
        all_rows=list(runbook_rows)+list(incident_rows)
        all_rows.sort(key=lambda r: r.distance) #ascending distance = descending similarity

        #convert each row to a RetrievedChunk. The similarity_score conversion (1 - distance) is what makes the score intuitive downstream: 1.0 = identical, 0.0 = orthogonal, anything negative would be opposite.
        #pgvector cosine distance is in [0, 2], so similarity is in [-1, 1]. With L2-normalized vectors and text content, we almost always see distances in [0, 1] which maps to similarities in [0, 1].
        chunks=[RetrievedChunk(source_type=row.source_type, source_ref=row.source_ref,chunk_text=row.chunk_text,similarity_score=1.0-row.distance,)
                for row in all_rows]

        log.info("retrieval_completed", run_id=run_id, chunks_retrieved=len(chunks), runbooks=len(runbook_rows), incidents=len(incident_rows))
        return {"retrieved_context": chunks}

    except Exception as e:
        #broad catch on purpose: retrieval failures (embedding API error, DB connection blip, pgvector quirk) should never crash the graph.
        #empty list flows downstream cleanly because retrieved_context defaults to [] in initial_state, so Recovery Planning handles "no retrieved context" uniformly whether retrieval succeeded with zero hits or failed entirely.
        log.warning("retrieval_failed_returning_empty_context", run_id=run_id, error=str(e))
        return {"retrieved_context": []}