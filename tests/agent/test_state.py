"""Sentinels are what the graph uses when an LLM node fails. nodes.py returns them instead
of raising, so their contents matter. recovery_plan_sentinel() must always be 'escalate',
the safe choice when the agent can't reason about a failure. classification_sentinel()
must be 'unknown' with confidence 0.0, which tells recovery planning to lean toward
escalate. If either changed, a failed run could quietly recommend an unsafe automatic
action instead of handing it to a human.
"""
from worker.app.agent.state import (
    classification_sentinel,
    log_analysis_sentinel,
    recovery_plan_sentinel,
)


def test_log_analysis_sentinel_flags_itself_as_degraded():
    sentinel = log_analysis_sentinel("ConnectionTimeout", "timed out after 30s")

    assert sentinel.failed_step == "unknown"
    assert "log_analysis_failed" in sentinel.notable_signals
    # the raw error still has to reach downstream nodes, so the sentinel
    # passes it through verbatim inside error_interpretation.
    assert "ConnectionTimeout" in sentinel.error_interpretation
    assert "timed out after 30s" in sentinel.error_interpretation


def test_classification_sentinel_is_unknown_with_zero_confidence():
    sentinel = classification_sentinel()

    assert sentinel.failure_classification == "unknown"
    assert sentinel.confidence == 0.0


def test_recovery_plan_sentinel_always_escalates():
    sentinel = recovery_plan_sentinel()

    assert sentinel.recommended_action == "escalate"
    assert sentinel.explanation  # non-empty, so a human has something to read
