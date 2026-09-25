from langchain_google_genai import ChatGoogleGenerativeAI
from shared.config import GOOGLE_API_KEY, GEMINI_MODEL
from shared.observability import get_logger
from worker.app.agent.state import (DiagnosticState,log_analysis_sentinel,classification_sentinel,recovery_plan_sentinel,)
from worker.app.agent.schemas import (LogAnalysisOutput,ClassificationOutput,RecoveryPlanOutput,)
from worker.app.agent.prompts import (log_analysis_prompt,classification_prompt,recovery_planning_prompt,tool_decision_prompt,render_retrieved_context,)
from worker.app.agent.tools import get_circuit_breaker_state

log=get_logger(__name__)

#one shared LLM client at module load: it's stateless and thread-safe, so reusing the same
#instance avoids re-authentication overhead on every invocation. temperature=0 keeps outputs
#deterministic for a given input, which matters for the eval harness later. All three nodes
#use the same model (gemini-2.5-flash). If we ever want to specialize, e.g. a heavier model
#for classification and a faster one for log analysis, this is the one spot to change.

_llm=ChatGoogleGenerativeAI(model=GEMINI_MODEL,google_api_key=GOOGLE_API_KEY,temperature=0)

#each node gets its own structured chain, built once at module load.
#with_structured_output() per node constrains each LLM call to produce JSON matching
#its specific output schema, so the model cannot return the wrong shape. This is
#the reliability win that makes multi-node graphs viable: each call has a tight constraint
#instead of one big call trying to populate everything.

_log_analysis_chain=log_analysis_prompt|_llm.with_structured_output(LogAnalysisOutput)
_classification_chain=classification_prompt|_llm.with_structured_output(ClassificationOutput)
_recovery_planning_chain=recovery_planning_prompt|_llm.with_structured_output(RecoveryPlanOutput)

#separate chain for the tool-decision step ahead of recovery planning: bind_tools() and
#with_structured_output() don't compose into a single chain, so this is a separate call whose
#(possible) tool result gets rendered into the final chain's tool_results_block input above,
#the same way retrieved_context_block already works. See tools.py and the TOOL_DECISION_*
#prompts in prompts.py for why this specific tool exists.
_tool_decision_chain=tool_decision_prompt|_llm.bind_tools([get_circuit_breaker_state])


# ============================================================================
# Node functions.
#
# LangGraph contract for each node:
#   - Signature: async def x_node(state: DiagnosticState) -> dict
#   - The returned dict is MERGED into state by LangGraph. Each node returns
#     a dict containing only the single field it's responsible for populating.
#   - Exceptions inside a node will propagate and crash the graph if not
#     caught here, so graceful degradation means catching and returning the
#     sentinel. Downstream nodes get a well-typed input either way.
#
# The log calls at the end of each node are intentional: they give visibility
# during local dev, so it's possible to see at a glance which node succeeded
# vs which fell back to a sentinel.
# ============================================================================


async def log_analysis_node(state: DiagnosticState) -> dict:
    """First node in the graph. Describes what happened; does not classify or recommend.

    Reads: error_type, error_message, run_context_json, pipeline_id, run_id (from state inputs)
    Writes: log_analysis (a LogAnalysisOutput or its sentinel)
    """
    run_id=state["run_id"]

    try:
        result: LogAnalysisOutput=await _log_analysis_chain.ainvoke({
            "pipeline_id": state["pipeline_id"],
            "run_id": run_id,
            "error_type": state["error_type"],
            "error_message": state["error_message"],
            "run_context_json": state["run_context_json"],
        })
        log.info("log_analysis_completed", run_id=run_id, failed_step=result.failed_step)
        return {"log_analysis": result}

    except Exception as e:
        #broad catch because LLM calls fail in many shapes: network errors, rate limits,
        #content filter blocks, malformed structured outputs, API key issues. None of those
        #should crash the graph. The sentinel keeps run_context flowing to downstream nodes
        #so they can still produce something useful (likely an escalate).
        log.warning("log_analysis_failed_using_sentinel", run_id=run_id, error=str(e))
        return {"log_analysis": log_analysis_sentinel(state["error_type"], state["error_message"])}


async def classification_node(state: DiagnosticState) -> dict:
    """Second node. Assigns a category + confidence based on log analysis and raw error metadata.

    Reads: error_type, error_message, log_analysis (from previous node)
    Writes: classification (a ClassificationOutput or its sentinel)
    """
    run_id=state["run_id"]
    log_analysis=state["log_analysis"]

    #if log_analysis is somehow None here, something is structurally wrong with the graph wiring,
    #not just an LLM hiccup. Still degrade gracefully rather than crash: return the sentinel
    #directly without even calling the LLM, since there's nothing to feed it.
    if log_analysis is None:
        log.warning("classification_skipped_log_analysis_missing", run_id=run_id)
        return {"classification": classification_sentinel()}

    try:
        result: ClassificationOutput=await _classification_chain.ainvoke({
            "error_type": state["error_type"],
            "error_message": state["error_message"],
            "failed_step": log_analysis.failed_step,
            "attempt_pattern": log_analysis.attempt_pattern,
            "error_interpretation": log_analysis.error_interpretation,
            #notable_signals is a list. Python's str() rendering of a list is readable enough
            #for the prompt ('[\"log_analysis_failed\"]'), and the LLM handles it fine.
            "notable_signals": log_analysis.notable_signals,
        })
        log.info("classification_completed", run_id=run_id, classification=result.failure_classification, confidence=round(result.confidence, 2))
        return {"classification": result}

    except Exception as e:
        log.warning("classification_failed_using_sentinel", run_id=run_id, error=str(e))
        return {"classification": classification_sentinel()}


async def recovery_planning_node(state: DiagnosticState) -> dict:
    """Third node. Recommends a recovery action based on everything upstream.

    Reads: error_type, error_message, log_analysis, classification (from prior nodes)
    Writes: recovery_plan (a RecoveryPlanOutput or its sentinel)

    Before the final structured call, a separate tool-decision call lets the model choose
    whether to invoke get_circuit_breaker_state (see tools.py) for this pipeline's live state.
    The model decides per-case whether the tool is relevant, and skips it for classifications
    where circuit breaker state doesn't matter.
    """
    run_id=state["run_id"]
    log_analysis=state["log_analysis"]
    classification=state["classification"]

    #same defensive None check as classification_node: if either upstream output is missing,
    #the graph wiring is broken, so we sentinel-out without burning an LLM call.
    if log_analysis is None or classification is None:
        log.warning("recovery_planning_skipped_upstream_missing", run_id=run_id)
        return {"recovery_plan": recovery_plan_sentinel()}

    #tool-decision step: the model decides for itself whether to call get_circuit_breaker_state
    #before the final recommendation. This is a separate, best-effort call. If it fails for any
    #reason (network, rate limit, malformed tool-call response), we fall back to the default
    #"not performed" text and still proceed to the final recovery plan, same posture as every
    #other LLM call here: a failed tool check degrades grounding, it doesn't crash the node.
    tool_results_block="No circuit breaker check was performed for this recommendation."
    try:
        tool_decision=await _tool_decision_chain.ainvoke({
            "pipeline_id": state["pipeline_id"],
            "failure_classification": classification.failure_classification,
            "confidence": classification.confidence,
            "error_type": state["error_type"],
            "attempt_pattern": log_analysis.attempt_pattern,
        })
        for call in getattr(tool_decision, "tool_calls", None) or []:
            if call["name"]=="get_circuit_breaker_state":
                tool_results_block=await get_circuit_breaker_state.ainvoke(call["args"])
                log.info("recovery_planning_tool_call", run_id=run_id, tool="get_circuit_breaker_state", args=call["args"])
    except Exception as e:
        log.warning("recovery_planning_tool_decision_failed", run_id=run_id, error=str(e))

    try:
        retrieved_context_block=render_retrieved_context(state["retrieved_context"])
        result: RecoveryPlanOutput=await _recovery_planning_chain.ainvoke({
            "error_type": state["error_type"],
            "error_message": state["error_message"],
            "failed_step": log_analysis.failed_step,
            "attempt_pattern": log_analysis.attempt_pattern,
            "error_interpretation": log_analysis.error_interpretation,
            "notable_signals": log_analysis.notable_signals,
            "failure_classification": classification.failure_classification,
            "confidence": classification.confidence,
            "reasoning": classification.reasoning,
            "retrieved_context_block": retrieved_context_block,
            "tool_results_block": tool_results_block,
        })
        log.info("recovery_planning_completed", run_id=run_id, recommended_action=result.recommended_action)
        return {"recovery_plan": result}

    except Exception as e:
        log.warning("recovery_planning_failed_using_sentinel", run_id=run_id, error=str(e))
        return {"recovery_plan": recovery_plan_sentinel()}