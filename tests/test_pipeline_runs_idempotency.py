"""create_pipeline_run_service (control_plane/app/services/pipeline_runs.py) handles
idempotency in two ways, and each is tested on its own.

  1. Fast path: a SELECT finds an existing run for (pipeline_id, idempotency_key) and
     returns it without trying an INSERT.
  2. Race path: the SELECT finds nothing because two requests arrived at nearly the same
     time, the INSERT hits the database's unique constraint, and the service queries again
     for the row that won instead of passing the IntegrityError to the caller.

session.execute, add, commit, refresh and rollback are all mocked. These tests check the
function's own control flow, not SQLAlchemy or Postgres. I checked the real unique
constraint with two concurrent requests against the live system by hand before writing
this file.
"""
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.exc import IntegrityError

from control_plane.app.services.pipeline_runs import create_pipeline_run_service


def make_pipeline(pipeline_id=1, tenant_id=1):
    return SimpleNamespace(id=pipeline_id, tenant_id=tenant_id)


def make_run(run_id=1, pipeline_id=1, idempotency_key=None):
    return SimpleNamespace(id=run_id, pipeline_id=pipeline_id, idempotency_key=idempotency_key)


def make_session(execute_results, commit_side_effect=None):
    """execute_results: list of values, one per session.execute() call, in call order.
    Each is wrapped so `.scalar_one_or_none()` returns it directly."""
    session = AsyncMock()
    results = iter(execute_results)

    async def execute(*args, **kwargs):
        value = next(results)
        return SimpleNamespace(scalar_one_or_none=lambda: value)

    session.execute = AsyncMock(side_effect=execute)
    session.add = lambda obj: None
    session.commit = AsyncMock(side_effect=commit_side_effect)
    session.refresh = AsyncMock()
    session.rollback = AsyncMock()
    return session


async def test_returns_none_false_when_the_pipeline_does_not_exist():
    session = make_session(execute_results=[None])

    run, created = await create_pipeline_run_service(tenant_id=1, pipeline_id=1, session=session)

    assert (run, created) == (None, False)


async def test_no_idempotency_key_always_creates_a_new_run():
    session = make_session(execute_results=[make_pipeline()])

    run, created = await create_pipeline_run_service(tenant_id=1, pipeline_id=1, session=session)

    assert created is True
    session.commit.assert_awaited_once()


async def test_a_fresh_idempotency_key_creates_a_new_run():
    # execute() calls in order: pipeline lookup, then the upfront idempotency-key SELECT (finds nothing)
    session = make_session(execute_results=[make_pipeline(), None])

    run, created = await create_pipeline_run_service(tenant_id=1, pipeline_id=1, session=session, idempotency_key="key-abc")

    assert created is True
    session.commit.assert_awaited_once()


async def test_a_key_that_already_exists_is_returned_without_inserting():
    existing = make_run(run_id=42, idempotency_key="key-abc")
    session = make_session(execute_results=[make_pipeline(), existing])

    run, created = await create_pipeline_run_service(tenant_id=1, pipeline_id=1, session=session, idempotency_key="key-abc")

    assert (run, created) == (existing, False)
    session.commit.assert_not_awaited()  # the fast path must never attempt an insert


async def test_a_true_race_recovers_via_the_unique_constraint_instead_of_raising():
    # upfront SELECT finds nothing (both requests race past it), INSERT hits the DB's unique
    # constraint, the recovery SELECT then finds the row the OTHER request just committed.
    winner = make_run(run_id=99, idempotency_key="key-abc")
    session = make_session(
        execute_results=[make_pipeline(), None, winner],
        commit_side_effect=IntegrityError("stmt", {}, Exception("duplicate key")),
    )

    run, created = await create_pipeline_run_service(tenant_id=1, pipeline_id=1, session=session, idempotency_key="key-abc")

    assert (run, created) == (winner, False)
    session.rollback.assert_awaited_once()


async def test_an_integrity_error_unrelated_to_idempotency_is_not_swallowed():
    # the recovery SELECT finds nothing either, so this wasn't an idempotency-key
    # collision, and the error must propagate rather than be discarded.
    session = make_session(
        execute_results=[make_pipeline(), None, None],
        commit_side_effect=IntegrityError("stmt", {}, Exception("some other constraint")),
    )

    with pytest.raises(IntegrityError):
        await create_pipeline_run_service(tenant_id=1, pipeline_id=1, session=session, idempotency_key="key-abc")

    session.rollback.assert_awaited_once()


async def test_an_integrity_error_with_no_idempotency_key_propagates_immediately():
    # no idempotency_key means there's no recovery SELECT to attempt, so any IntegrityError
    # here is an unrelated constraint violation and must surface as-is.
    session = make_session(
        execute_results=[make_pipeline()],
        commit_side_effect=IntegrityError("stmt", {}, Exception("some other constraint")),
    )

    with pytest.raises(IntegrityError):
        await create_pipeline_run_service(tenant_id=1, pipeline_id=1, session=session)

    session.rollback.assert_awaited_once()
