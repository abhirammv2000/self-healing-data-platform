from pydantic import BaseModel, Field
from typing import Literal, List

#these are the structured outputs we expect the LLM to produce
#they are SEPARATE from the API-facing schemas in control_plane/app/schemas/agent_recommendations.py because the LLM output is an INTERNAL contract between Claude/Gemini and our code,
#while the API schema is the EXTERNAL contract between our API and tenants.
#even though the fields overlap right now, keeping them separate means we can evolve the LLM output (add reasoning fields, confidence scores, citations) 
# without affecting the public API shape, and vice versa.

#these Literals MUST match the design doc's failure categories and recovery actions exactly. Otherwise the agent will produce values that don't map to anything downstream
FailureClassification=Literal["network", "quota", "schema", "partial_load", "unknown"]
RecommendedAction=Literal["retry", "retry_with_backoff", "schema_evolution", "replay_from_raw", "escalate", "pause_schedule"]

# ============================================================================
# Multi-agent schemas — one output model per LangGraph node.
# Each model is intentionally focused on what its node actually produces,
# nothing more. Keeping them small makes each LLM call's structured-output
# constraint tighter, which improves reliability of the generated JSON.
# ============================================================================

class LogAnalysisOutput(BaseModel):
    """Output of the Log Analysis node.

    This node DESCRIBES what happened — it does NOT classify or recommend. That separation is intentional: if this node also classified, the downstream classification node would just be 
    rubber-stamping its conclusion. Keeping interpretation and categorization in different nodes gives each LLM call a single, focused job.
    """

    failed_step: str=Field(description="The name of the pipeline step that failed (e.g. 'ingestion', 'validation', 'transformation', 'load'). "
                    "Pulled from the step_attempts trail in run_context. Use 'unknown' if the step cannot be determined.")
    attempt_pattern: str=Field(description="A description of the retry/attempt pattern observed. "
                    "Examples: 'failed on first attempt with no retries configured', "
                    "'failed 3 times with the same error after exponential backoff', "
                    "'succeeded on retry attempt 2 but then a later step failed'. "
                    "This is the interpretive piece — describe what the attempt trail tells you.", min_length=10, max_length=500)
    error_interpretation: str=Field(description="What the error type and message semantically suggest about the root cause. "
                    "Stay descriptive — say what the error LOOKS like, not what category it belongs to. "
                    "Example: 'A connection timeout while fetching from an external HTTP source' "
                    "rather than 'a network failure'.", min_length=10, max_length=500)
    notable_signals: List[str]=Field(default_factory=list,description="Optional free-form list of anything else worth flagging — timing anomalies, "
                    "partial completion indicators, unusual config values, etc. Empty list if nothing stands out. "
                    "Also used as a breadcrumb channel: degraded outputs include sentinel strings like 'log_analysis_failed'.")


class ClassificationOutput(BaseModel):
    """Output of the Failure Classification node.

    This node receives the log analysis output AND the original error metadata,
    and produces a single categorical decision plus confidence. The confidence
    field is what gives the eval harness a quantitative signal and enables
    future conditional routing ('if confidence < 0.5, escalate immediately').
    """

    failure_classification: FailureClassification=Field(description="The category of failure. Must be one of the allowed values.")
    confidence: float=Field(description="How confident the classification is, on a 0.0 to 1.0 scale. "
                    "Anchor: 0.9+ when error type and message are unambiguous (e.g. 'ConnectionTimeout' clearly maps to 'network'). "
                    "0.5-0.8 when evidence points to a category but isn't airtight. "
                    "Below 0.5 when the evidence is conflicting or weak — in those cases lean toward 'unknown'.", ge=0.0, le=1.0)
    reasoning: str=Field(description="Why this classification was chosen, grounded in the log analysis output and original error metadata. "
                    "Reference specific signals (error type, failed step, attempt pattern).", min_length=20, max_length=1000)


class RecoveryPlanOutput(BaseModel):
    """Output of the Recovery Planning node.

    This is the node whose output most directly drives operator behavior — the
    recommended_action is the concrete suggestion, and the explanation is what
    a human reads to decide whether to apply or dismiss the recommendation.
    """

    recommended_action: RecommendedAction=Field(description="The specific recovery action to recommend. Must be one of the allowed values.")
    explanation: str=Field(description="A clear, prose explanation that weaves together the log analysis, classification, and your recovery reasoning. "
                    "This will be shown to a human operator and stored in the AgentRecommendation.explanation column. "
                    "Reference specific evidence — failed step, error type, classification, attempt pattern. "
                    "If the upstream nodes produced degraded outputs (e.g. classification='unknown' with confidence 0.0), "
                    "acknowledge that and lean toward 'escalate'.", min_length=20, max_length=2000)