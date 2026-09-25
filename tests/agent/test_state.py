"""Sentinel outputs are what the graph falls back to when an LLM node fails.
worker/app/agent/nodes.py returns these instead of propagating an exception,
so it's worth locking down exactly what they contain. In particular:
recovery_plan_sentinel() must always be 'escalate' (the safe default when the
agent can't reason about a failure), and classification_sentinel() must be
'unknown' with confidence 0.0 (the signal recovery planning uses to lean
toward escalate). If either drifted, a degraded run could silently recommend
an unsafe automated action instead of kicking it to a human.
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
