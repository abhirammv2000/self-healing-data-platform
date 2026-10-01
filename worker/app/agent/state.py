from typing import TypedDict, Optional, List, Literal
from pydantic import BaseModel, Field
from worker.app.agent.schemas import (LogAnalysisOutput,ClassificationOutput,RecoveryPlanOutput,)

#why TypedDict and not Pydantic for the graph state:
#TypedDict is the idiomatic LangGraph state container. Every official example uses it.
#LangGraph's state-merging semantics work with TypedDict natively. Switching to a Pydantic model would add a layer of wrapping for no real benefit at this scale.
#The individual node OUTPUTS stay as Pydantic models (above) because with_structured_output() needs them but those outputs are then stored INSIDE the TypedDict state.


# ============================================================================
# RetrievedChunk lives here rather than in schemas.py because it's a state-shape
# concern, not an LLM-output concern. schemas.py holds the structured outputs
# we constrain Gemini to produce; RetrievedChunk is what our deterministic
# retrieval_node produces from pgvector and writes into state.
# Keeping it next to DiagnosticState makes the coupling visible.
# ============================================================================

class RetrievedChunk(BaseModel):
    """One row of retrieved context from the incident_embeddings table.

    Written by retrieval_node, read by recovery_planning_node. The node renders these into a numbered list in the Recovery Planning prompt, and the LLM is instructed to cite them by source_ref in its explanation.
    """
    source_type: Literal["past_run", "past_recommendation", "runbook"]=Field(description="Which kind of source produced this chunk. The Recovery Planning prompt uses this to weight or contextualize the citation — "
                    "runbook chunks are operational knowledge, past_run/past_recommendation chunks are tenant-specific history.")
    source_ref: str=Field(description="Citation handle. For runbooks, this is 'filename.md#section'. For incidents, 'run_id=N' or 'rec_id=N'. The agent uses this string verbatim when citing sources in its explanation field.")
    chunk_text: str=Field(description="The exact text that was embedded. Passed through to the LLM so it can reason over the content, not just the citation.")
    similarity_score: float=Field(description="Converted from pgvector's cosine distance via (1 - distance). Higher = more similar. We convert in the retrieval node so this field is uniformly intuitive everywhere downstream.",
                    ge=-1.0, le=1.0,)
    
class DiagnosticState(TypedDict):
    """The state object that flows through the LangGraph diagnostic graph.

    Input fields are populated once at graph entry and never modified. Output fields are filled in by their own nodes. Each field is written by exactly one node, so no reducer functions are needed.

    All output fields except retrieved_context are Optional because the graph builds them incrementally: when the graph starts, they're None; after their node runs, they're populated.
    retrieved_context is the exception. It defaults to [] and not None, so downstream code (Recovery Planning) treats "no retrieved context" the same whether retrieval found nothing or failed. That means less branching.
    """

    # inputs (set once at graph entry)
    run_id: int
    pipeline_id: int
    tenant_id: int
    error_type: str
    error_message: str
    #pre-serialized once at graph entry, reused by all three nodes' prompts.
    #avoids re-running json.dumps three times and keeps state simple to checkpoint.
    run_context_json: str

    # accumulated outputs (populated by nodes)
    log_analysis: Optional[LogAnalysisOutput]
    classification: Optional[ClassificationOutput]
    #retrieved_context is populated by the new retrieval_node (between classification and recovery_planning).
    #empty list = no relevant context found OR retrieval failed. Recovery Planning handles both the same way.
    retrieved_context: List[RetrievedChunk]
    recovery_plan: Optional[RecoveryPlanOutput]


# ============================================================================
# Sentinel outputs for graceful degradation.
#
# When a node fails (LLM error, structured-output validation error, network
# blip, whatever), it returns its sentinel instead of propagating the exception.
# Downstream nodes still get a well-typed input to work with, and the final
# recommendation row still gets written, clearly marked as degraded.
#
# The breadcrumb is in two places:
#   1. notable_signals contains a string like 'log_analysis_failed'
#   2. confidence=0.0 / recommended_action='escalate' for the downstream nodes
# A human reading the recommendation can immediately tell the agent didn't
# function fully and that escalation is the safe call.
# ============================================================================

def log_analysis_sentinel(error_type:str, error_message:str)->LogAnalysisOutput:
    """Returned by log_analysis_node on any failure.

    We still pass the raw error through in error_interpretation so the next nodes have *something* to work with. The notable_signals breadcrumb is how downstream nodes (and humans) 
    know this output is degraded.
    """
    return LogAnalysisOutput(failed_step="unknown",attempt_pattern="Could not analyze step attempts due to log analysis failure.",
                             error_interpretation=f"Raw error passed through without interpretation: {error_type} - {error_message}", notable_signals=["log_analysis_failed"])

def classification_sentinel() -> ClassificationOutput:
    """Returned by classification_node on any failure.

    'unknown' is already a valid category in our Literal. Perfect for 'we genuinely don't know'. Confidence 0.0 makes the degraded state explicit and is the signal Recovery Planning 
    uses to lean toward escalate.
    """
    return ClassificationOutput(failure_classification="unknown", confidence=0.0,reasoning="Classification step failed; defaulting to 'unknown' with zero confidence.")


def recovery_plan_sentinel() -> RecoveryPlanOutput:
    """Returned by recovery_planning_node on any failure.

    'escalate' is exactly what 'we can't safely recommend automation' should look like. It kicks the decision to a human operator.
    """
    return RecoveryPlanOutput(recommended_action="escalate",explanation="Recovery planning step failed; recommending human review of this pipeline run failure.")