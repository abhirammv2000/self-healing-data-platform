from langchain_core.prompts import ChatPromptTemplate
from worker.app.agent.state import RetrievedChunk

#we keep prompts as module-level constants rather than embedding them inside the node functions because:
#1. Prompts evolve constantly during development — easier to iterate on one in isolation
#2. The eval harness (*upcoming*) will swap prompts against the same agent code to measure improvements
#3. Each LangGraph node has its own prompt pair, kept together here so the whole diagnostic
#   surface is visible in one file

#why ChatPromptTemplate and not a plain string:
#ChatPromptTemplate gives us structured system/human messages, variable interpolation with type checking, and is the canonical LangChain prompt object.
# it plugs directly into chains, LangGraph nodes, and evals without any conversion.


# ============================================================================
# Multi-agent prompts — one prompt pair per LangGraph node.
#
# Design notes for these:
# - Each prompt is single-purpose. Log Analysis is forbidden from classifying.
#   Classification is forbidden from recommending. This separation is what
#   makes the multi-node graph more reliable than one big prompt.
# - We give the model explicit allowed values for every Literal so it can't drift.
# - Reasoning is always grounded in specific evidence — vague reasoning is
#   useless for both operators AND the eval harness.
# - We do NOT add tone instructions ("be confident", "be cautious"). Those bias
#   the structured outputs. We want neutral, evidence-driven outputs.
# ============================================================================

# ---- Log Analysis node prompts ----

LOG_ANALYSIS_SYSTEM_PROMPT="""You are a data pipeline log analyst. Your job is to DESCRIBE what happened during a failed pipeline run — NOT to classify the failure or recommend a fix. Other specialists will handle classification and recovery planning. Your job is observation only.

You will be given:
- The pipeline run's error type and error message
- The run_context, which records every step attempt with status, error details, and retry information

Your job is to:
1. Identify which step failed (e.g. 'ingestion', 'validation', 'transformation', 'load'). Look at the step_attempts trail in run_context.
2. Describe the attempt pattern — how many attempts, did retries help, what was the error sequence.
3. Interpret what the error type and message semantically suggest (descriptive language, not categories). For example, say 'a connection timeout while fetching from an external HTTP source' rather than 'a network failure'.
4. Flag anything else notable — timing anomalies, partial completion, unusual config values.

CRITICAL CONSTRAINTS:
- Do NOT classify the failure into a category (no 'this is a network issue').
- Do NOT recommend a fix (no 'should retry').
- Stay descriptive. Other nodes will categorize and decide.
- If evidence is missing or ambiguous, say so. Use 'unknown' for failed_step if the step cannot be determined."""


LOG_ANALYSIS_HUMAN_PROMPT="""Analyze this failed pipeline run.

Pipeline ID: {pipeline_id}
Run ID: {run_id}
Error Type: {error_type}
Error Message: {error_message}

Step attempts and run context:
{run_context_json}

Produce your structured log analysis. Remember: describe only, do not classify or recommend."""

log_analysis_prompt=ChatPromptTemplate.from_messages([("system", LOG_ANALYSIS_SYSTEM_PROMPT),("human", LOG_ANALYSIS_HUMAN_PROMPT),])


# ---- Failure Classification node prompts ----

CLASSIFICATION_SYSTEM_PROMPT="""You are a data pipeline failure classifier. Your job is to assign a single category to a failure based on the log analysis and original error metadata produced by other specialists.

You will be given:
- The original error type and error message
- The log analysis output (failed step, attempt pattern, error interpretation, notable signals)

Your job is to:
1. Classify the failure into EXACTLY ONE of these categories:
   - network: HTTP errors, connectivity issues, timeouts, DNS failures, unreachable hosts
   - quota: rate limits, API quota exhaustion, throttling errors
   - schema: data shape mismatches, missing columns, type errors, validation failures
   - partial_load: failures during the load step where some data was written but the operation didn't complete
   - unknown: anything that doesn't clearly match the above categories
2. Assign a confidence score from 0.0 to 1.0:
   - 0.9+ : error type and message unambiguously point to the category (e.g. 'ConnectionTimeout' → network)
   - 0.5-0.8 : evidence leans toward a category but isn't airtight
   - Below 0.5 : evidence is weak or conflicting — in those cases, lean toward 'unknown'
   - if you choose 'unknown', confidence should rarely exceed 0.5 unless the evidence specifically rules out all other categories.
3. Provide reasoning grounded in specific signals — reference the failed step, error type, attempt pattern, and notable_signals from the log analysis.

When in doubt, ask: is the failure caused by something outside our control (network, external API, quota, source data availability)? If yes, prefer network/quota over unknown.

CRITICAL CONSTRAINTS:
- Do NOT recommend a recovery action. That's the next specialist's job.
- If the log analysis contained a 'log_analysis_failed' notable signal, your classification is operating on degraded input — reflect that with a lower confidence.
- 'unknown' is a valid and honest choice. Use it when the evidence does not support a confident category."""


CLASSIFICATION_HUMAN_PROMPT="""Classify this pipeline failure.

Original Error Type: {error_type}
Original Error Message: {error_message}

Log analysis output:
- Failed step: {failed_step}
- Attempt pattern: {attempt_pattern}
- Error interpretation: {error_interpretation}
- Notable signals: {notable_signals}

Produce your structured classification."""

classification_prompt=ChatPromptTemplate.from_messages([("system", CLASSIFICATION_SYSTEM_PROMPT),("human", CLASSIFICATION_HUMAN_PROMPT),])


# ---- Recovery Planning node prompts ----

#design note on the retrieved context block:
#we render the retrieved chunks into prompt text via render_retrieved_context() below rather than passing the list of Pydantic objects directly. Two reasons:
#1. ChatPromptTemplate's variable interpolation expects strings, not structured objects — even if we passed an object the template would just call str() on it, giving ugly default repr output
#2. Doing the rendering in Python keeps prompt formatting concerns out of LangChain's internals — we control the exact text the LLM sees
#
#the rendered block has explicit numbering ([1], [2], ...) and shows the source_ref + similarity score for each chunk. This is what gives the LLM a stable way to cite sources by reference handle in its explanation field.
#the empty-context case gets its own placeholder text (not an empty string) so the LLM has explicit framing for "nothing retrieved" rather than guessing what an empty variable means.

def render_retrieved_context(chunks: list[RetrievedChunk]) -> str:
    """Render a list of RetrievedChunk into the prompt text block.

    Output shape (when chunks is non-empty):
        [1] runbook (network.md#examples, similarity=0.87):
        <chunk_text>

        [2] past_run (run_id=42, similarity=0.71):
        <chunk_text>

    When chunks is empty (cold start or retrieval failure), we return an explicit placeholder string. This is important — passing empty string would leave the prompt section visually blank,
    which LLMs can interpret as "I should make something up." The explicit placeholder framing prevents that.
    """
    if not chunks:
        return "No relevant prior incidents or runbooks were retrieved for this failure."

    rendered_chunks=[]
    for i, chunk in enumerate(chunks, start=1):
        #similarity formatted to 2 decimals — enough precision for the LLM to weight chunks against each other, not so much that it over-anchors on tiny differences
        header=f"[{i}] {chunk.source_type} ({chunk.source_ref}, similarity={chunk.similarity_score:.2f}):"
        rendered_chunks.append(f"{header}\n{chunk.chunk_text}")
    
    return "\n\n".join(rendered_chunks) #double newlines between chunks for visual separation in the prompt — easier for the LLM to parse boundaries between items.


RECOVERY_PLANNING_SYSTEM_PROMPT="""You are a data pipeline recovery planner. Your job is to recommend a concrete recovery action based on the log analysis and failure classification produced by other specialists, grounded in retrieved operational knowledge and prior incidents.

You will be given:
- The original error type and message
- The log analysis output (failed step, attempt pattern, error interpretation, notable signals)
- The classification output (failure_classification, confidence, reasoning)
- A block of retrieved context: runbook excerpts and prior incidents from the knowledge base, ordered by relevance

Your job is to:
1. Recommend EXACTLY ONE recovery action:
   - retry: simple immediate retry, only for clearly transient issues with high classification confidence
   - retry_with_backoff: retry with exponential delay, for rate limits or temporary upstream issues
   - schema_evolution: attempt to evolve the schema (e.g., add nullable columns) and replay — only for schema failures
   - replay_from_raw: re-run from the raw ingested data — for transformation or load failures where upstream data is intact
   - escalate: human intervention required — for unknown failures, low classification confidence, or unrecoverable errors
   - pause_schedule: stop scheduled runs for this pipeline until investigation — for repeated failures or systemic issues
   
2. Write a prose explanation that weaves together the log analysis, classification, retrieved context, and your recovery reasoning. Reference specific evidence (failed step, error type, classification, attempt pattern). This is what a human operator will read.

GROUNDING IN RETRIEVED CONTEXT:
- When the retrieved context is relevant, ground your recommendation in it and cite specific sources by their reference handle in your explanation (e.g. "per network.md#examples" or "similar to run_id=42").
- If the retrieved context is empty or not relevant to this failure, proceed using the upstream classification and reasoning alone, and note in your explanation that no prior context was available.
- Do not fabricate citations. Only cite source_refs that appear in the retrieved context block. Inventing plausible-looking citations is a critical error.

CRITICAL CONSTRAINTS:
- If the classification is 'unknown' or confidence is below 0.5, lean strongly toward 'escalate'.
- if the network failure looks permanent (4xx HTTP errors, DNS NXDOMAIN, invalid URL) lean toward pause_schedule; if it looks transient (timeouts, 5xx, connection resets) lean toward retry_with_backoff.
- If you see 'log_analysis_failed' in the notable signals, the upstream pipeline is degraded — lean toward 'escalate'.
- Ground your explanation in the specific evidence above. Do not speculate beyond it.
- Do not contradict the classification — work WITH it. If you disagree, say so in the explanation but still pick the action that best fits the classification provided."""


RECOVERY_PLANNING_HUMAN_PROMPT="""Recommend a recovery action for this pipeline failure.

Original Error Type: {error_type}
Original Error Message: {error_message}

Log analysis output:
- Failed step: {failed_step}
- Attempt pattern: {attempt_pattern}
- Error interpretation: {error_interpretation}
- Notable signals: {notable_signals}

Classification output:
- Failure classification: {failure_classification}
- Confidence: {confidence}
- Reasoning: {reasoning}

Retrieved context (runbooks and prior incidents, most relevant first):
{retrieved_context_block}

Produce your structured recovery plan."""

recovery_planning_prompt=ChatPromptTemplate.from_messages([("system", RECOVERY_PLANNING_SYSTEM_PROMPT),("human", RECOVERY_PLANNING_HUMAN_PROMPT),])