"""retrieval_node has two jobs worth locking down independently:

1. _build_query(): the text that gets embedded and searched on. It has a
   defensive branch for when classification/log_analysis are None (state
   is TypedDict-typed as always-populated by the time retrieval runs, but the
   function still guards against the impossible case), which is easy to break
   silently since nothing else exercises that branch.
2. retrieval_node() itself: combines two SQL queries (runbooks, tenant-scoped
   incidents) into one globally similarity-sorted list, converts pgvector
   cosine distance to an intuitive similarity score, and follows the same
   graceful-degradation contract as the LLM nodes, returning an empty list
   rather than raising if the embedding call or either query fails.

Both the DB session and the embedding call are mocked; a real pgvector
instance is not something a unit test should depend on.
"""
from types import SimpleNamespace
from unittest.mock import AsyncMock

from worker.app.agent import retrieval_node as rn


def make_row(source_type, source_ref, chunk_text, distance):
    return SimpleNamespace(source_type=source_type, source_ref=source_ref, chunk_text=chunk_text, distance=distance)


class FakeSessionCM:
    """Fakes `async with async_session() as session: ...`.

    execute() is called twice by retrieval_node (runbook query, then incident
    query); results are supplied in call order via `execute_results`.
    """
    def __init__(self, execute_results):
        self._results = iter(execute_results)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    async def execute(self, *args, **kwargs):
        rows = next(self._results)
        return SimpleNamespace(all=lambda: rows)


def patch_session(monkeypatch, execute_results):
    monkeypatch.setattr(rn, "async_session", lambda: FakeSessionCM(execute_results))


# ---------------------------------------------------------------------------
# _build_query
# ---------------------------------------------------------------------------

def test_build_query_uses_upstream_classification_and_log_analysis():
    state = {
        "classification": SimpleNamespace(failure_classification="network"),
        "log_analysis": SimpleNamespace(failed_step="ingestion", attempt_pattern="failed once, no retries"),
        "error_type": "ConnectionTimeout",
        "error_message": "timed out after 30s",
    }

    query = rn._build_query(state)

    assert "network failure during ingestion step" in query
    assert "ConnectionTimeout - timed out after 30s" in query
    assert "failed once, no retries" in query


def test_build_query_degrades_gracefully_when_upstream_outputs_are_none():
    """Shouldn't normally happen (both nodes have sentinel fallbacks upstream),
    but _build_query has its own None-guard, so it's worth confirming it
    doesn't crash if it's ever reached, e.g. a future graph rewiring bug."""
    state = {
        "classification": None,
        "log_analysis": None,
        "error_type": "ConnectionTimeout",
        "error_message": "timed out",
    }

    query = rn._build_query(state)

    assert "unknown failure during unknown step" in query


# ---------------------------------------------------------------------------
# retrieval_node
# ---------------------------------------------------------------------------

async def _run_with(monkeypatch, execute_results, embed_result=None, embed_side_effect=None):
    patch_session(monkeypatch, execute_results)
    monkeypatch.setattr(rn, "embed_text_async", AsyncMock(return_value=embed_result, side_effect=embed_side_effect))

    state = {
        "run_id": 1, "tenant_id": 7,
        "classification": SimpleNamespace(failure_classification="network"),
        "log_analysis": SimpleNamespace(failed_step="ingestion", attempt_pattern="failed once"),
        "error_type": "ConnectionTimeout", "error_message": "timed out",
    }
    return await rn.retrieval_node(state)


async def test_retrieval_node_combines_and_sorts_by_similarity(monkeypatch):
    runbook_rows = [make_row("runbook", "network.md#examples", "retry with backoff", distance=0.3)]
    incident_rows = [make_row("past_run", "run_id=42", "same timeout last week", distance=0.1)]

    result = await _run_with(monkeypatch, execute_results=[runbook_rows, incident_rows], embed_result=[0.1, 0.2])

    chunks = result["retrieved_context"]
    assert len(chunks) == 2
    # the closer (lower-distance) incident chunk should sort first
    assert chunks[0].source_ref == "run_id=42"
    assert chunks[0].similarity_score == 0.9  # 1 - 0.1
    assert chunks[1].source_ref == "network.md#examples"
    assert chunks[1].similarity_score == 0.7  # 1 - 0.3


async def test_retrieval_node_returns_empty_list_when_nothing_matches(monkeypatch):
    result = await _run_with(monkeypatch, execute_results=[[], []], embed_result=[0.1, 0.2])

    assert result == {"retrieved_context": []}


async def test_retrieval_node_swallows_embedding_failure(monkeypatch):
    result = await _run_with(monkeypatch, execute_results=[[], []], embed_side_effect=RuntimeError("embedding API down"))

    assert result == {"retrieved_context": []}


async def test_retrieval_node_swallows_db_failure(monkeypatch):
    monkeypatch.setattr(rn, "embed_text_async", AsyncMock(return_value=[0.1, 0.2]))

    class BrokenSessionCM:
        async def __aenter__(self):
            raise RuntimeError("db connection refused")
        async def __aexit__(self, *args):
            return False

    monkeypatch.setattr(rn, "async_session", lambda: BrokenSessionCM())

    state = {
        "run_id": 1, "tenant_id": 7,
        "classification": SimpleNamespace(failure_classification="network"),
        "log_analysis": SimpleNamespace(failed_step="ingestion", attempt_pattern="failed once"),
        "error_type": "ConnectionTimeout", "error_message": "timed out",
    }
    result = await rn.retrieval_node(state)

    assert result == {"retrieved_context": []}
