"""run_diagnostic_agent() is the one function the executor calls in its
`finally` block for every failed run. It must never raise, and it writes a
recommendation row on almost every path. These tests lock down the four
"return None, write nothing" paths, plus the success path, without touching
a database, an LLM, or the module-level compiled graph.
"""
from unittest.mock import AsyncMock, MagicMock

import pytest

from worker.app.agent import diagnostic_agent as da
from worker.app.agent.schemas import ClassificationOutput, RecoveryPlanOutput


class FakeSessionCM:
    """Fakes `async with async_session() as session: ...` for the persistence
    block. session.add() is sync, matching SQLAlchemy; commit/refresh are
    async. refresh() assigns a fixed id, standing in for where a DB would
    assign the row's id."""
    def __init__(self, commit_side_effect=None):
        self.added = []
        self.commit = AsyncMock(side_effect=commit_side_effect)
        self.rollback = AsyncMock()

    def add(self, obj):
        self.added.append(obj)

    async def refresh(self, obj):
        obj.id = 999

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False


def make_run_context(run_status="failed"):
    return {
        "run_id": 1, "pipeline_id": 2, "tenant_id": 3,
        "run_status": run_status,
        "error_type": "ConnectionTimeout", "error_message": "timed out",
    }


async def test_returns_none_immediately_for_a_non_failed_run(monkeypatch):
    # a successful run_context should never even reach the graph
    mock_ainvoke = AsyncMock()
    monkeypatch.setattr(da, "_compiled_graph", MagicMock(ainvoke=mock_ainvoke))

    result = await da.run_diagnostic_agent(make_run_context(run_status="success"))

    assert result is None
    mock_ainvoke.assert_not_awaited()


async def test_returns_none_when_run_context_is_not_json_serializable(monkeypatch):
    class Unserializable:
        def __str__(self):
            raise TypeError("cannot even stringify this")

    mock_ainvoke = AsyncMock()
    monkeypatch.setattr(da, "_compiled_graph", MagicMock(ainvoke=mock_ainvoke))

    run_context = make_run_context()
    run_context["poison"] = Unserializable()

    result = await da.run_diagnostic_agent(run_context)

    assert result is None
    mock_ainvoke.assert_not_awaited()


async def test_returns_none_when_graph_invocation_raises(monkeypatch):
    monkeypatch.setattr(da, "_compiled_graph", MagicMock(ainvoke=AsyncMock(side_effect=RuntimeError("langgraph internal error"))))

    result = await da.run_diagnostic_agent(make_run_context())

    assert result is None


async def test_returns_none_and_writes_nothing_when_final_state_missing_outputs(monkeypatch):
    # classification present but recovery_plan missing: the documented
    # "impossible but not zero" case. Should skip persistence rather than
    # write a half-filled row.
    final_state = {"classification": ClassificationOutput(failure_classification="network", confidence=0.9, reasoning="x" * 25), "recovery_plan": None}
    monkeypatch.setattr(da, "_compiled_graph", MagicMock(ainvoke=AsyncMock(return_value=final_state)))

    session_spy = FakeSessionCM()
    monkeypatch.setattr(da, "async_session", lambda: session_spy)

    result = await da.run_diagnostic_agent(make_run_context())

    assert result is None
    assert session_spy.added == []


async def test_success_path_persists_recommendation_and_indexes_incident(monkeypatch):
    classification = ClassificationOutput(failure_classification="network", confidence=0.9, reasoning="ConnectionTimeout during ingestion, unambiguous." )
    recovery_plan = RecoveryPlanOutput(recommended_action="retry_with_backoff", explanation="Transient network failure, retry with backoff as per runbook.")
    final_state = {"classification": classification, "recovery_plan": recovery_plan}
    monkeypatch.setattr(da, "_compiled_graph", MagicMock(ainvoke=AsyncMock(return_value=final_state)))

    session_spy = FakeSessionCM()
    monkeypatch.setattr(da, "async_session", lambda: session_spy)

    mock_index_incident = AsyncMock()
    monkeypatch.setattr(da, "index_incident", mock_index_incident)

    result = await da.run_diagnostic_agent(make_run_context())

    assert result == 999  # the id FakeSessionCM.refresh() assigned
    assert len(session_spy.added) == 1
    written = session_spy.added[0]
    assert written.failure_classification == "network"
    assert written.recommended_action == "retry_with_backoff"
    # fire-and-forget indexing task was scheduled with the persisted id
    mock_index_incident.assert_called_once()
    assert mock_index_incident.call_args.kwargs["recommendation_id"] == 999


async def test_returns_none_when_persistence_raises(monkeypatch):
    from sqlalchemy.exc import SQLAlchemyError

    classification = ClassificationOutput(failure_classification="network", confidence=0.9, reasoning="x" * 25)
    recovery_plan = RecoveryPlanOutput(recommended_action="retry_with_backoff", explanation="y" * 25)
    final_state = {"classification": classification, "recovery_plan": recovery_plan}
    monkeypatch.setattr(da, "_compiled_graph", MagicMock(ainvoke=AsyncMock(return_value=final_state)))

    session_spy = FakeSessionCM(commit_side_effect=SQLAlchemyError("db write failed"))
    monkeypatch.setattr(da, "async_session", lambda: session_spy)

    result = await da.run_diagnostic_agent(make_run_context())

    assert result is None
    session_spy.rollback.assert_awaited_once()
