"""worker/app/agent/nodes.py's three LLM nodes share one contract: on success
they return {"<field>": <parsed Pydantic output>}, on any exception from the
chain they return {"<field>": <sentinel>()} instead of letting the exception
propagate and crash the graph. classification_node and recovery_planning_node
have a second contract on top of that: if their required upstream input is
None, they must return the sentinel without calling the LLM at all, since
there's nothing meaningful to send it.

The module-level chains (_log_analysis_chain etc.) are built once at import
time from a ChatGoogleGenerativeAI client, and each is a LangChain
RunnableSequence: a Pydantic model, so it rejects setting an arbitrary
`.ainvoke` attribute on the instance (Pydantic validates field names on
__setattr__). So instead of patching a method onto the chain object, every
test here swaps out the whole module-level chain for a small fake with an
async ainvoke(), via monkeypatch.setattr(nodes, "_log_analysis_chain", ...).
That keeps every test away from the real Gemini API.
"""
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from worker.app.agent import nodes
from worker.app.agent.schemas import (
    ClassificationOutput,
    LogAnalysisOutput,
    RecoveryPlanOutput,
)


class FakeChain:
    """Stand-in for a `prompt | llm.with_structured_output(...)` RunnableSequence.

    Just enough surface for the nodes: an async ainvoke(). Not a Pydantic model,
    so it happily accepts whatever mock we hand it.
    """
    def __init__(self, ainvoke_mock):
        self.ainvoke = ainvoke_mock


def make_ai_message_with_tool_calls(tool_calls):
    """Stand-in for what `prompt | llm.bind_tools([...])` returns: an AIMessage-like
    object whose .tool_calls is a list of {"name", "args", "id"} dicts. SimpleNamespace
    is enough since recovery_planning_node only ever reads .tool_calls off this."""
    return SimpleNamespace(tool_calls=tool_calls)


NO_TOOL_CALLS = make_ai_message_with_tool_calls([])


def make_log_analysis_output(**overrides):
    defaults = dict(
        failed_step="ingestion",
        attempt_pattern="failed on first attempt with no retries configured",
        error_interpretation="a connection timeout while fetching from an external HTTP source",
        notable_signals=[],
    )
    defaults.update(overrides)
    return LogAnalysisOutput(**defaults)


def make_classification_output(**overrides):
    defaults = dict(failure_classification="network", confidence=0.95, reasoning="ConnectionTimeout during ingestion unambiguously points to network.")
    defaults.update(overrides)
    return ClassificationOutput(**defaults)


def make_recovery_plan_output(**overrides):
    defaults = dict(recommended_action="retry_with_backoff", explanation="Transient network failure during ingestion; retry with backoff.")
    defaults.update(overrides)
    return RecoveryPlanOutput(**defaults)


# ---------------------------------------------------------------------------
# log_analysis_node
# ---------------------------------------------------------------------------

async def test_log_analysis_node_returns_chain_output_on_success(monkeypatch):
    expected = make_log_analysis_output()
    monkeypatch.setattr(nodes, "_log_analysis_chain", FakeChain(AsyncMock(return_value=expected)))

    result = await nodes.log_analysis_node({
        "run_id": 1, "pipeline_id": 1, "error_type": "ConnectionTimeout",
        "error_message": "timed out", "run_context_json": "{}",
    })

    assert result == {"log_analysis": expected}


async def test_log_analysis_node_falls_back_to_sentinel_on_chain_failure(monkeypatch):
    monkeypatch.setattr(nodes, "_log_analysis_chain", FakeChain(AsyncMock(side_effect=RuntimeError("rate limited"))))

    result = await nodes.log_analysis_node({
        "run_id": 1, "pipeline_id": 1, "error_type": "ConnectionTimeout",
        "error_message": "timed out", "run_context_json": "{}",
    })

    sentinel = result["log_analysis"]
    assert "log_analysis_failed" in sentinel.notable_signals
    assert "ConnectionTimeout" in sentinel.error_interpretation


# ---------------------------------------------------------------------------
# classification_node
# ---------------------------------------------------------------------------

async def test_classification_node_returns_chain_output_on_success(monkeypatch):
    expected = make_classification_output()
    mock_ainvoke = AsyncMock(return_value=expected)
    monkeypatch.setattr(nodes, "_classification_chain", FakeChain(mock_ainvoke))

    result = await nodes.classification_node({
        "run_id": 1, "error_type": "ConnectionTimeout", "error_message": "timed out",
        "log_analysis": make_log_analysis_output(),
    })

    assert result == {"classification": expected}
    mock_ainvoke.assert_awaited_once()


async def test_classification_node_skips_the_llm_when_log_analysis_missing(monkeypatch):
    mock_ainvoke = AsyncMock()
    monkeypatch.setattr(nodes, "_classification_chain", FakeChain(mock_ainvoke))

    result = await nodes.classification_node({
        "run_id": 1, "error_type": "ConnectionTimeout", "error_message": "timed out",
        "log_analysis": None,
    })

    assert result["classification"].failure_classification == "unknown"
    assert result["classification"].confidence == 0.0
    mock_ainvoke.assert_not_awaited()  # no LLM call should happen with nothing to feed it


async def test_classification_node_falls_back_to_sentinel_on_chain_failure(monkeypatch):
    monkeypatch.setattr(nodes, "_classification_chain", FakeChain(AsyncMock(side_effect=RuntimeError("bad json"))))

    result = await nodes.classification_node({
        "run_id": 1, "error_type": "ConnectionTimeout", "error_message": "timed out",
        "log_analysis": make_log_analysis_output(),
    })

    assert result["classification"].failure_classification == "unknown"
    assert result["classification"].confidence == 0.0


# ---------------------------------------------------------------------------
# recovery_planning_node
# ---------------------------------------------------------------------------

def make_recovery_state(**overrides):
    state = {
        "run_id": 1, "pipeline_id": 1, "error_type": "ConnectionTimeout", "error_message": "timed out",
        "log_analysis": make_log_analysis_output(),
        "classification": make_classification_output(),
        "retrieved_context": [],
    }
    state.update(overrides)
    return state


async def test_recovery_planning_node_returns_chain_output_on_success(monkeypatch):
    expected = make_recovery_plan_output()
    mock_ainvoke = AsyncMock(return_value=expected)
    monkeypatch.setattr(nodes, "_recovery_planning_chain", FakeChain(mock_ainvoke))
    monkeypatch.setattr(nodes, "_tool_decision_chain", FakeChain(AsyncMock(return_value=NO_TOOL_CALLS)))

    result = await nodes.recovery_planning_node(make_recovery_state())

    assert result == {"recovery_plan": expected}
    mock_ainvoke.assert_awaited_once()
    # tool wasn't called, so the final chain should get the default "not performed" text
    assert mock_ainvoke.call_args.args[0]["tool_results_block"] == "No circuit breaker check was performed for this recommendation."


@pytest.mark.parametrize("missing_field", ["log_analysis", "classification"])
async def test_recovery_planning_node_skips_the_llm_when_upstream_output_missing(monkeypatch, missing_field):
    mock_ainvoke = AsyncMock()
    monkeypatch.setattr(nodes, "_recovery_planning_chain", FakeChain(mock_ainvoke))
    tool_mock = AsyncMock(return_value=NO_TOOL_CALLS)
    monkeypatch.setattr(nodes, "_tool_decision_chain", FakeChain(tool_mock))

    result = await nodes.recovery_planning_node(make_recovery_state(**{missing_field: None}))

    assert result["recovery_plan"].recommended_action == "escalate"
    mock_ainvoke.assert_not_awaited()
    tool_mock.assert_not_awaited()  # no LLM call of any kind should happen with nothing to feed it


async def test_recovery_planning_node_falls_back_to_sentinel_on_chain_failure(monkeypatch):
    monkeypatch.setattr(nodes, "_recovery_planning_chain", FakeChain(AsyncMock(side_effect=RuntimeError("timeout"))))
    monkeypatch.setattr(nodes, "_tool_decision_chain", FakeChain(AsyncMock(return_value=NO_TOOL_CALLS)))

    result = await nodes.recovery_planning_node(make_recovery_state())

    assert result["recovery_plan"].recommended_action == "escalate"


# ---------------------------------------------------------------------------
# recovery_planning_node: tool-decision step
# ---------------------------------------------------------------------------

async def test_recovery_planning_node_uses_the_tool_result_when_the_model_calls_it(monkeypatch):
    # get_circuit_breaker_state is a @tool-decorated BaseTool, a Pydantic model like the
    # chains, so it rejects setting an arbitrary .ainvoke attribute on the instance. Swap
    # the whole name in nodes' namespace for a fake, the same pattern used for the chains.
    tool_call = {"name": "get_circuit_breaker_state", "args": {"pipeline_id": 1}, "id": "call_1"}
    monkeypatch.setattr(nodes, "_tool_decision_chain", FakeChain(AsyncMock(return_value=make_ai_message_with_tool_calls([tool_call]))))
    tool_fn_mock = AsyncMock(return_value="Circuit breaker for pipeline_id=1: state=open, failure_reason=3 failures in 60 min")
    monkeypatch.setattr(nodes, "get_circuit_breaker_state", FakeChain(tool_fn_mock))

    final_mock = AsyncMock(return_value=make_recovery_plan_output())
    monkeypatch.setattr(nodes, "_recovery_planning_chain", FakeChain(final_mock))

    await nodes.recovery_planning_node(make_recovery_state())

    tool_fn_mock.assert_awaited_once_with({"pipeline_id": 1})
    assert "state=open" in final_mock.call_args.args[0]["tool_results_block"]


async def test_recovery_planning_node_ignores_tool_calls_for_unrecognized_tool_names(monkeypatch):
    # defensive: if bind_tools() ever returns a call for a tool this node doesn't know about
    # (future tools added to the same bind_tools list, a model quirk), it should be skipped
    # rather than crash on an unexpected name.
    tool_call = {"name": "some_other_tool", "args": {}, "id": "call_1"}
    monkeypatch.setattr(nodes, "_tool_decision_chain", FakeChain(AsyncMock(return_value=make_ai_message_with_tool_calls([tool_call]))))
    final_mock = AsyncMock(return_value=make_recovery_plan_output())
    monkeypatch.setattr(nodes, "_recovery_planning_chain", FakeChain(final_mock))

    await nodes.recovery_planning_node(make_recovery_state())

    assert final_mock.call_args.args[0]["tool_results_block"] == "No circuit breaker check was performed for this recommendation."


async def test_recovery_planning_node_proceeds_when_the_tool_decision_step_itself_fails(monkeypatch):
    monkeypatch.setattr(nodes, "_tool_decision_chain", FakeChain(AsyncMock(side_effect=RuntimeError("rate limited"))))
    final_mock = AsyncMock(return_value=make_recovery_plan_output())
    monkeypatch.setattr(nodes, "_recovery_planning_chain", FakeChain(final_mock))

    result = await nodes.recovery_planning_node(make_recovery_state())

    # the tool-decision failure must not crash the node or skip the final recommendation
    assert result["recovery_plan"].recommended_action == "retry_with_backoff"
    assert final_mock.call_args.args[0]["tool_results_block"] == "No circuit breaker check was performed for this recommendation."


async def test_recovery_planning_node_proceeds_when_the_tool_call_itself_fails(monkeypatch):
    tool_call = {"name": "get_circuit_breaker_state", "args": {"pipeline_id": 1}, "id": "call_1"}
    monkeypatch.setattr(nodes, "_tool_decision_chain", FakeChain(AsyncMock(return_value=make_ai_message_with_tool_calls([tool_call]))))
    monkeypatch.setattr(nodes, "get_circuit_breaker_state", FakeChain(AsyncMock(side_effect=RuntimeError("db down"))))

    final_mock = AsyncMock(return_value=make_recovery_plan_output())
    monkeypatch.setattr(nodes, "_recovery_planning_chain", FakeChain(final_mock))

    result = await nodes.recovery_planning_node(make_recovery_state())

    assert result["recovery_plan"].recommended_action == "retry_with_backoff"
    assert final_mock.call_args.args[0]["tool_results_block"] == "No circuit breaker check was performed for this recommendation."
