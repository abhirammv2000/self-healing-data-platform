import json
from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver
from sqlalchemy.exc import SQLAlchemyError
from shared.db import async_session
from control_plane.app.models.agent_recommendations import AgentRecommendation
from worker.app.agent.state import DiagnosticState
from worker.app.agent.nodes import (log_analysis_node,classification_node,recovery_planning_node,)
from worker.app.agent.retrieval_node import retrieval_node
import asyncio
from worker.app.agent.index_incident import index_incident

# ============================================================================
# Graph construction — happens ONCE at module load. Compiled graphs are
# stateless across invocations (state lives in the checkpointer and in the
# per-invoke initial state dict), so reusing the same compiled graph for every
# failed run is safe and avoids graph construction overhead on every call.
# ============================================================================

def _build_graph():
    """Wire the four diagnostic nodes into a linear LangGraph.

    Topology: log_analysis → classification → retrieval → recovery_planning → END
    Linear and explicit — no conditional edges, no cycles. That's the right
    starting point per our design discussion: conditional branching is earned
    later when we have concrete reasons (low-confidence skip-to-escalate,
    anomaly warning mode, etc.), not added speculatively.

    retrieval is a deterministic (non-LLM) node that pulls runbook and past-incident
    context from pgvector for the recovery_planning node to ground its recommendation in.
    """
    graph=StateGraph(DiagnosticState)

    #register each node as a named graph node. The names are what we reference
    #when wiring edges below — keep them stable, they show up in logs and traces.
    graph.add_node("log_analysis", log_analysis_node)
    graph.add_node("classification", classification_node)
    graph.add_node("retrieval", retrieval_node)
    graph.add_node("recovery_planning", recovery_planning_node)

    #entry point: the graph starts here when invoked.
    graph.set_entry_point("log_analysis")

    #linear edges. Each node's output flows into the next node's state.
    graph.add_edge("log_analysis", "classification")
    graph.add_edge("classification", "retrieval")
    graph.add_edge("retrieval", "recovery_planning")
    graph.add_edge("recovery_planning", END)

    #MemorySaver per our design call — in-process checkpointing, lost on restart.
    #fine for MVP: diagnostic runs complete in a few seconds with no human-in-the-loop
    #pauses. Swapping to PostgresSaver later is a one-line change if we ever want
    #durable checkpointing.
    return graph.compile(checkpointer=MemorySaver())


_compiled_graph=_build_graph()


# ============================================================================
# Public entry point — the executor imports and awaits this from its finally
# block. The whole function is defensive: it catches its own errors and returns
# None on any failure, so it can never break the executor.
# ============================================================================

async def run_diagnostic_agent(run_context: dict) -> int | None:
    """Diagnose a failed pipeline run via the multi-agent graph and persist a recommendation.

    Called by the executor in the finally block, AFTER the run has been marked as failed
    and observability has been recorded, BEFORE the webhook is dispatched. The returned
    recommendation_id (or None) is used by the webhook payload's recommendations_url.

    The graph runs four nodes in sequence:
        log_analysis → classification → retrieval → recovery_planning

    Each LLM node has graceful degradation (returns a sentinel on failure), so by the time
    the graph completes, the classification and recovery_plan outputs are populated — with
    real outputs OR sentinels. This means we ALMOST ALWAYS write a recommendation, even when
    the agent is partially degraded. The recommendation row honestly reflects what the agent
    could and couldn't figure out (sentinels lean toward 'escalate' to kick degraded cases
    to humans).

    The only paths that return None (no recommendation written):
        1. run_context is not from a failed run (caller passed in a successful run by mistake)
        2. run_context fails to serialize to JSON (extremely unlikely, defensive only)
        3. The graph itself crashes (e.g. langgraph internal error — not a node failure)
        4. The recommendation persistence to DB fails

    Returns the new AgentRecommendation.id on success, or None on the above failure modes.
    """

    #defensive: only run for failed runs. Lets the executor call us unconditionally
    #in its finally block without needing to inspect run_status itself.
    if run_context.get("run_status")!="failed":
        return None

    run_id=run_context.get("run_id")
    pipeline_id=run_context.get("pipeline_id")
    tenant_id=run_context.get("tenant_id")
    error_type=run_context.get("error_type","Unknown")
    error_message=run_context.get("error_message","No error message available")

    #serialize run_context once at the boundary. All three nodes read the same JSON
    #from state — no point re-running json.dumps three times. default=str handles
    #datetime objects gracefully without it, json.dumps would throw on the first datetime.
    try:
        run_context_json=json.dumps(run_context,indent=2,default=str)
    except Exception as e:
        print(f"Agent: failed to serialize run_context for run_id={run_id}: {e}")
        return None

    #build the initial state for the graph. Output fields start as None — each
    #node writes its slot, LangGraph merges those writes into the running state.
    initial_state: DiagnosticState={
        "run_id": run_id,
        "pipeline_id": pipeline_id,
        "tenant_id": tenant_id,
        "error_type": error_type,
        "error_message": error_message,
        "run_context_json": run_context_json,
        "log_analysis": None,
        "classification": None,
        "retrieved_context": [],
        "recovery_plan": None,
    }

    #the checkpointer needs a thread_id to associate state checkpoints with a logical
    #conversation/run. Using str(run_id) is natural — it's unique per diagnostic, and
    #if we ever want to resume a checkpoint post-MVP, we can look it up by run_id.
    config={"configurable": {"thread_id": str(run_id)}}

    #invoke the graph. We catch broad Exception here because we want the same defensive
    #posture as the legacy implementation — the agent is best-effort. Individual node
    #failures are already absorbed by their own try/except + sentinels, so reaching this
    #catch means something structural broke (langgraph internals, our wiring, etc.).
    try:
        final_state=await _compiled_graph.ainvoke(initial_state,config=config)
    except Exception as e:
        print(f"Agent: graph invocation failed for run_id={run_id}: {e}")
        return None

    #with graceful degradation, all three outputs should always be populated by now —
    #but defend against the impossible-but-not-zero case where the graph somehow returned
    #without populating recovery_plan or classification. Better to return None than write
    #a half-filled recommendation row to the DB.
    classification=final_state.get("classification")
    recovery_plan=final_state.get("recovery_plan")
    if classification is None or recovery_plan is None:
        print(f"Agent: graph completed but missing required outputs for run_id={run_id} — skipping persistence")
        return None

    #persist the recommendation in a FRESH session — same pattern as the legacy code and
    #the webhook dispatcher. The agent shouldn't inherit the executor's session state.
    #We use recovery_plan.explanation directly per our earlier design call — option (a):
    #trust the recovery planning node to weave together the upstream reasoning into clean
    #prose, rather than mechanically concatenating fields from all three nodes.
    async with async_session() as session:
        try:
            recommendation=AgentRecommendation(
                tenant_id=tenant_id,
                pipeline_id=pipeline_id,
                run_id=run_id,
                failure_classification=classification.failure_classification,
                recommended_action=recovery_plan.recommended_action,
                explanation=recovery_plan.explanation,
                #status defaults to "pending" on the model — the tenant decides what to do
                #with the recommendation (apply it, dismiss it). The agent never auto-sets
                #status beyond pending, even when it recommends 'escalate'.
            )
            session.add(recommendation)
            await session.commit()
            await session.refresh(recommendation)

            #fire-and-forget incident indexing. Same pattern as the webhook dispatcher in the executor —
            #the indexing happens in the background; the agent returns the recommendation_id immediately
            #so the executor's webhook payload can use it. If indexing fails, it's logged but doesn't
            #affect this run's response.
            asyncio.create_task(index_incident(run_context=run_context,recommendation_id=recommendation.id,failure_classification=classification.failure_classification,
                                               recommended_action=recovery_plan.recommended_action,explanation=recovery_plan.explanation))

            print(f"Agent: wrote recommendation_id={recommendation.id} for run_id={run_id} "
                  f"({classification.failure_classification} → {recovery_plan.recommended_action})")
            return recommendation.id

        except SQLAlchemyError as e:
            await session.rollback()
            print(f"Agent: failed to persist recommendation for run_id={run_id}: {e}")
            return None