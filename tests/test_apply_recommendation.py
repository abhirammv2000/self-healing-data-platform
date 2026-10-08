"""Approving an agent recommendation does the safe part of what it recommended.

Until now "applied" was only a label. Now a person approving a retry starts a run, approving
pause_schedule turns the schedule off, and approving anything else only records the decision, because
those actions change data or need judgement. The status changes in the same commit as the action, so a
failed action leaves the recommendation pending. As in test_pipeline_runs_idempotency.py the session is
mocked: these tests check the control flow, not SQLAlchemy or Postgres.
"""
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.exc import SQLAlchemyError

from control_plane.app.schemas.agent_recommendations import AgentRecommendationUpdate
from control_plane.app.services import agent_recommendations as service


def make_rec(action="retry", status="pending", rec_id=7):
    return SimpleNamespace(id=rec_id, tenant_id=1, pipeline_id=3, run_id=11, recommended_action=action, status=status,
                           updated_at=None)


def make_session(*execute_results):
    session = AsyncMock()
    results = iter(execute_results)

    async def execute(*args, **kwargs):
        return SimpleNamespace(scalar_one_or_none=lambda: next(results))

    session.execute = AsyncMock(side_effect=execute)
    session.commit = AsyncMock()
    session.refresh = AsyncMock()
    session.rollback = AsyncMock()
    return session


def update(status):
    return AgentRecommendationUpdate(status=status)


@pytest.fixture
def started(monkeypatch):
    """Replace run creation, and record how it was called."""
    calls = []

    async def fake_create(tenant_id, pipeline_id, session, idempotency_key=None):
        calls.append((tenant_id, pipeline_id, idempotency_key))
        return SimpleNamespace(id=42), True

    monkeypatch.setattr(service, "create_pipeline_run_service", fake_create)
    return calls


@pytest.mark.parametrize("action", ["retry", "retry_with_backoff"])
async def test_approving_a_retry_starts_a_run_and_asks_for_it_to_be_queued(action, started):
    rec = make_rec(action)
    session = make_session(rec)

    result = await service.update_agent_recommendation_service(1, 7, update("applied"), session)

    assert started == [(1, 3, "recommendation-7")]
    assert result.status == "applied" and result.applied_effect == "started run 42" and result.enqueue_run_id == 42
    session.commit.assert_awaited_once()


async def test_the_run_is_keyed_by_the_recommendation_so_a_second_approval_cannot_start_another(monkeypatch):
    async def replay(tenant_id, pipeline_id, session, idempotency_key=None):
        return SimpleNamespace(id=42), False  # the run for this key already exists

    monkeypatch.setattr(service, "create_pipeline_run_service", replay)
    rec = make_rec("retry")

    result = await service.update_agent_recommendation_service(1, 7, update("applied"), make_session(rec))

    assert result.enqueue_run_id is None and "already started" in result.applied_effect


async def test_a_pipeline_that_no_longer_exists_leaves_the_recommendation_pending(monkeypatch):
    async def gone(*args, **kwargs):
        return None, False

    monkeypatch.setattr(service, "create_pipeline_run_service", gone)
    rec = make_rec("retry")
    session = make_session(rec)

    with pytest.raises(ValueError, match="no longer exists"):
        await service.update_agent_recommendation_service(1, 7, update("applied"), session)

    assert rec.status == "pending" and session.commit.await_count == 0


async def test_approving_pause_schedule_turns_the_schedule_off():
    schedule = SimpleNamespace(is_active=True)
    rec = make_rec("pause_schedule")

    result = await service.update_agent_recommendation_service(1, 7, update("applied"), make_session(rec, schedule))

    assert schedule.is_active is False and result.applied_effect == "paused the schedule"


async def test_pause_schedule_with_no_schedule_says_so():
    rec = make_rec("pause_schedule")

    result = await service.update_agent_recommendation_service(1, 7, update("applied"), make_session(rec, None))

    assert "nothing to pause" in result.applied_effect and result.status == "applied"


@pytest.mark.parametrize("action", ["schema_evolution", "replay_from_raw", "escalate"])
async def test_the_other_actions_are_only_recorded(action, started):
    rec = make_rec(action)

    result = await service.update_agent_recommendation_service(1, 7, update("applied"), make_session(rec))

    assert started == [] and result.enqueue_run_id is None
    assert result.status == "applied" and "recorded only" in result.applied_effect and action in result.applied_effect


async def test_dismissing_does_nothing_but_change_the_status(started):
    rec = make_rec("retry")

    result = await service.update_agent_recommendation_service(1, 7, update("dismissed"), make_session(rec))

    assert started == [] and result.status == "dismissed" and result.applied_effect is None


@pytest.mark.parametrize("current", ["applied", "dismissed"])
async def test_a_decision_is_final(current, started):
    rec = make_rec("retry", status=current)
    session = make_session(rec)

    with pytest.raises(ValueError, match=f"already {current}"):
        await service.update_agent_recommendation_service(1, 7, update("applied"), session)

    assert started == [] and session.commit.await_count == 0


async def test_it_cannot_be_moved_back_to_pending(started):
    with pytest.raises(ValueError, match="back to pending"):
        await service.update_agent_recommendation_service(1, 7, update("pending"), make_session(make_rec()))

    assert started == []


async def test_another_tenants_recommendation_is_not_found(started):
    result = await service.update_agent_recommendation_service(2, 7, update("applied"), make_session(None))

    assert result is None and started == []


async def test_a_database_error_rolls_back_and_the_action_is_not_reported(started):
    rec = make_rec("escalate")
    session = make_session(rec)
    session.commit = AsyncMock(side_effect=SQLAlchemyError("down"))

    with pytest.raises(SQLAlchemyError):
        await service.update_agent_recommendation_service(1, 7, update("applied"), session)

    session.rollback.assert_awaited_once()


# the route: a started run is put on the queue the worker reads

async def test_the_route_queues_the_run_the_recommendation_started(monkeypatch):
    from control_plane.app.routes import agent_recommendations as route

    pushed = []

    async def rpush(queue, value):
        pushed.append((queue, value))

    async def fake_service(tenant_id, rec_id, rec_data, session):
        return SimpleNamespace(enqueue_run_id=42)

    monkeypatch.setattr(route, "redis_client", SimpleNamespace(rpush=rpush))
    monkeypatch.setattr(route, "update_agent_recommendation_service", fake_service)

    result = await route.update_agent_recommendation(1, 7, update("applied"), session=None)

    assert pushed == [("pipeline_runs", "42")] and result.enqueue_run_id == 42


async def test_the_route_queues_nothing_when_no_run_was_started(monkeypatch):
    from control_plane.app.routes import agent_recommendations as route

    pushed = []

    async def fake_service(tenant_id, rec_id, rec_data, session):
        return SimpleNamespace(enqueue_run_id=None)

    monkeypatch.setattr(route, "redis_client", SimpleNamespace(rpush=AsyncMock(side_effect=lambda *a: pushed.append(a))))
    monkeypatch.setattr(route, "update_agent_recommendation_service", fake_service)

    await route.update_agent_recommendation(1, 7, update("dismissed"), session=None)

    assert pushed == []


async def test_the_route_tells_the_caller_when_the_queue_is_down(monkeypatch):
    from fastapi import HTTPException

    from control_plane.app.routes import agent_recommendations as route

    async def fake_service(tenant_id, rec_id, rec_data, session):
        return SimpleNamespace(enqueue_run_id=42)

    monkeypatch.setattr(route, "redis_client", SimpleNamespace(rpush=AsyncMock(side_effect=RuntimeError("redis down"))))
    monkeypatch.setattr(route, "update_agent_recommendation_service", fake_service)

    with pytest.raises(HTTPException) as error:
        await route.update_agent_recommendation(1, 7, update("applied"), session=None)

    assert error.value.status_code == 500 and "Start the run by hand" in error.value.detail


async def test_the_route_reports_an_already_decided_recommendation_as_a_client_error(monkeypatch):
    from fastapi import HTTPException

    from control_plane.app.routes import agent_recommendations as route

    async def fake_service(tenant_id, rec_id, rec_data, session):
        raise ValueError("this recommendation is already applied")

    monkeypatch.setattr(route, "update_agent_recommendation_service", fake_service)

    with pytest.raises(HTTPException) as error:
        await route.update_agent_recommendation(1, 7, update("applied"), session=None)

    assert error.value.status_code == 400 and "already applied" in error.value.detail
