"""How worker/app/executor.py runs one pipeline run, from claiming it to the webhook at the end.

These tests describe what the executor does, so a change to it can be checked against them. They cover the
success path, retries with backoff, a run that keeps failing, the circuit breaker (closed, half-open and open),
a run that cannot be claimed, and the things that must never break a run: the run record on disk, the
diagnostic agent and the webhook.

The database is a small fake that answers each query by looking at the SQL it was given, so the tests do not
depend on the order of the queries. The steps, the clock, the sleep, the agent and the webhook are replaced.
These tests check the executor's own decisions, not SQLAlchemy or Postgres.
"""
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from worker.app import executor
from worker.app.exceptions import InvalidPipelineRunStatusError, ObservabilityRecordingError, UnknownStepTypeError

NOW = datetime(2026, 1, 1, 12, 0, 0)


class FakeResult:
    def __init__(self, value=None, rowcount=0, items=None):
        self.value = value
        self.rowcount = rowcount
        self.items = items or []

    def scalar_one_or_none(self):
        return self.value

    def scalar_one(self):
        return self.value

    def one_or_none(self):
        return self.value

    def scalars(self):
        return SimpleNamespace(all=lambda: self.items)


class FakeDatabase:
    """One pipeline run, its steps, an optional circuit breaker and an optional pipeline."""

    def __init__(self, steps, breaker, run_status, callback_url, failed_runs):
        self.run = SimpleNamespace(
            id=1, pipeline_id=7, tenant_id=3, status=run_status, started_at=None, updated_at=None,
            ended_at=None, error_type=None, error_message=None, retry_count=0, attempts=0,
        )
        self.steps = steps
        self.breaker = breaker
        self.pipeline = SimpleNamespace(id=7, callback_url=callback_url)
        self.failed_runs = failed_runs
        self.commits = 0
        self.rollbacks = 0
        self.another_worker_first = False

    async def execute(self, statement):
        sql = str(statement)
        if sql.startswith("UPDATE pipeline_runs"):
            return self._update_run(statement)
        if sql.startswith("UPDATE pipeline_circuit_breakers"):
            return self._update_breaker(statement)
        if "FOR UPDATE" in sql:
            # the check that the worker still owns the run, which reads its attempt number and status
            return FakeResult(self.run)
        if "count(" in sql:
            return FakeResult(self.failed_runs)
        if "FROM pipeline_circuit_breakers" in sql:
            return FakeResult(self.breaker)
        if "FROM pipeline_steps" in sql:
            return FakeResult(items=self.steps)
        if "FROM pipeline_runs" in sql:
            return FakeResult(self.run)
        if "FROM pipelines" in sql:
            return FakeResult(self.pipeline)
        raise AssertionError(f"the executor ran a query this fake does not know: {sql}")

    def _update_run(self, statement):
        """An UPDATE with `WHERE status = <x>`: only changes the run if it is in that status."""
        params = statement.compile().params
        required_status = params["status_1"]
        if self.run is None or self.run.status != required_status:
            return FakeResult(rowcount=0)
        for column, value in params.items():
            if not column.endswith("_1") and column != "id":
                setattr(self.run, column, value)
        if params.get("status") == "running":  # a claim: counts the attempt and returns its number
            self.run.attempts += 1
            return FakeResult(value=self.run.attempts, rowcount=1)
        return FakeResult(rowcount=1)

    def _update_breaker(self, statement):
        """The open -> half-open UPDATE: changes the breaker only if it is open and its retry time has passed.

        `another_worker_first` plays two workers arriving together: the other one's UPDATE lands just before ours.
        """
        params = statement.compile().params
        if self.another_worker_first and self.breaker.state == "open":
            self.breaker.state = "half-open"
            return FakeResult(rowcount=0)
        if self.breaker.state != params["state_1"] or self.breaker.retry_after is None                 or self.breaker.retry_after > params["retry_after_1"]:
            return FakeResult(rowcount=0)
        self.breaker.state = params["state"]
        self.breaker.updated_at = params["updated_at"]
        return FakeResult(rowcount=1)

    async def commit(self):
        self.commits += 1

    async def rollback(self):
        self.rollbacks += 1

    async def refresh(self, _object):
        return None


class FakeSessionFactory:
    def __init__(self, database):
        self.database = database

    def __call__(self):
        return self

    async def __aenter__(self):
        return self.database

    async def __aexit__(self, *_):
        return False


class FakeLeaseKeeper:
    """Stands in for the heartbeat, which has its own tests in test_lease.py."""

    lost = False

    async def stop(self):
        pass


@dataclass
class Harness:
    database: FakeDatabase
    sleeps: list = field(default_factory=list)
    recorded: list = field(default_factory=list)
    webhooks: list = field(default_factory=list)
    agent: AsyncMock = None
    calls: list = field(default_factory=list)

    async def run(self):
        return await executor._run_executor_service(1)


def flaky(calls, name, failures):
    """A step that fails `failures` times and then works."""
    state = {"left": failures}

    async def step(config, context):
        calls.append(name)
        if state["left"] > 0:
            state["left"] -= 1
            raise RuntimeError("boom")

    return step


@pytest.fixture
def harness(monkeypatch):
    """Builds a run with the given steps and breaker, and replaces everything outside the executor."""

    def build(steps, breaker=None, run_status="queued", callback_url=None, failed_runs=0):
        database = FakeDatabase(steps, breaker, run_status, callback_url, failed_runs)
        h = Harness(database)

        async def fake_sleep(delay):
            h.sleeps.append(delay)

        async def fake_record(context):
            h.recorded.append(dict(context))

        def fake_webhook(*args):
            h.webhooks.append(args)
            return "sent"

        h.agent = AsyncMock(return_value=99)
        from worker.app.agent import diagnostic_agent

        monkeypatch.setattr(diagnostic_agent, "run_diagnostic_agent", h.agent)
        monkeypatch.setattr(executor, "async_session", FakeSessionFactory(database))
        monkeypatch.setattr(executor, "now_naive", lambda: NOW)
        monkeypatch.setattr(executor, "asyncio", SimpleNamespace(sleep=fake_sleep, create_task=lambda coro: coro))
        monkeypatch.setattr(executor, "start_lease_keeper", lambda run_id, attempt: FakeLeaseKeeper())
        monkeypatch.setattr(executor, "run_observability_recording", fake_record)
        monkeypatch.setattr(executor, "dispatch_webhook_callback", fake_webhook)

        async def ok(config, context):
            h.calls.append("ok")

        async def boom(config, context):
            h.calls.append("boom")
            raise RuntimeError("boom")

        monkeypatch.setitem(executor.step_registry, "ok", ok)
        monkeypatch.setitem(executor.step_registry, "boom", boom)
        monkeypatch.setitem(executor.step_registry, "flaky_once", flaky(h.calls, "flaky_once", 1))
        return h

    return build


def step(order, step_type, **config):
    return SimpleNamespace(step_order=order, step_type=step_type, config=config)


def breaker(state="closed", threshold=3, window=60, retry_after=NOW + timedelta(minutes=10)):
    """By default an open or half-open breaker still has ten minutes to wait."""
    return SimpleNamespace(
        pipeline_id=7, state=state, failure_reason="old reason" if state != "closed" else None,
        retry_after=retry_after if state != "closed" else None, updated_at=None,
        failure_count_threshold=threshold, failure_window_minutes=window,
    )


# ---- a run that works ----


async def test_a_run_claims_executes_every_step_and_ends_as_a_success(harness):
    h = harness([step(1, "ok"), step(2, "ok")])

    result = await h.run()

    assert result is h.database.run
    assert h.database.run.status == "success"
    assert h.database.run.started_at == NOW and h.database.run.ended_at == NOW
    assert h.calls == ["ok", "ok"]
    assert [a["status"] for a in h.recorded[0]["step_attempts"]] == ["success", "success"]
    assert h.recorded[0]["run_status"] == "success"


async def test_the_steps_get_their_config_and_a_context_that_names_the_run(harness, monkeypatch):
    seen = {}

    async def capture(config, context):
        seen["config"], seen["context"] = config, dict(context)

    h = harness([step(1, "capture", url="https://source")])
    monkeypatch.setitem(executor.step_registry, "capture", capture)

    await h.run()

    assert seen["config"] == {"url": "https://source"}
    assert (seen["context"]["run_id"], seen["context"]["pipeline_id"], seen["context"]["tenant_id"]) == (1, 7, 3)


async def test_the_run_is_marked_running_and_started_while_its_steps_execute(harness, monkeypatch):
    seen = {}

    async def watch(config, context):
        run = h.database.run
        seen["status"], seen["started_at"] = run.status, run.started_at

    h = harness([step(1, "watch")])
    monkeypatch.setitem(executor.step_registry, "watch", watch)

    await h.run()

    assert seen == {"status": "running", "started_at": NOW}


async def test_a_successful_run_leaves_a_closed_breaker_alone(harness):
    closed = breaker("closed")
    await harness([step(1, "ok")], closed).run()
    assert closed.state == "closed"


# ---- retries ----


async def test_a_step_that_fails_once_is_retried_after_the_backoff_delay(harness):
    h = harness([step(1, "flaky_once", retry={"max_retries": 2, "strategy": "exponential_backoff", "base_delay": 2})])

    await h.run()

    assert h.database.run.status == "success"
    assert h.database.run.retry_count == 1
    assert h.sleeps == [2]
    attempts = h.recorded[0]["step_attempts"]
    assert [(a["attempt"], a["status"], a["delay"]) for a in attempts] == [(1, "failed", 2), (2, "success", 0)]


async def test_backoff_grows_between_attempts_and_the_last_attempt_does_not_wait(harness):
    h = harness([step(1, "boom", retry={"max_retries": 3, "strategy": "exponential_backoff", "base_delay": 1})])

    with pytest.raises(RuntimeError, match="boom"):
        await h.run()

    assert h.sleeps == [1, 2, 4]
    assert [a["delay"] for a in h.recorded[0]["step_attempts"]] == [1, 2, 4, 0]
    assert h.database.run.retry_count == 3


async def test_a_step_with_no_retry_config_runs_exactly_once(harness):
    h = harness([step(1, "boom")])

    with pytest.raises(RuntimeError):
        await h.run()

    assert h.calls == ["boom"]
    assert h.sleeps == []


# ---- a run that keeps failing ----


async def test_a_run_that_runs_out_of_retries_is_recorded_as_failed_and_the_error_is_raised(harness):
    h = harness([step(1, "boom", retry={"max_retries": 1, "base_delay": 1})])

    with pytest.raises(RuntimeError, match="boom"):
        await h.run()

    run = h.database.run
    assert (run.status, run.error_type, run.error_message, run.ended_at) == ("failed", "RuntimeError", "boom", NOW)
    assert h.recorded[0]["run_status"] == "failed"
    assert h.recorded[0]["error_type"] == "RuntimeError"


async def test_steps_after_a_failed_step_do_not_run(harness):
    h = harness([step(1, "boom"), step(2, "ok")])

    with pytest.raises(RuntimeError):
        await h.run()

    assert h.calls == ["boom"]


async def test_an_unknown_step_type_fails_the_run_with_its_own_error_type(harness):
    h = harness([step(1, "no_such_step")])

    with pytest.raises(UnknownStepTypeError):
        await h.run()

    assert h.database.run.status == "failed"
    assert h.database.run.error_type == "UnknownStepTypeError"


# ---- the circuit breaker ----


async def test_enough_recent_failures_open_a_closed_breaker_and_say_why(harness):
    closed = breaker("closed", threshold=3, window=60)
    h = harness([step(1, "boom")], closed, failed_runs=3)

    with pytest.raises(RuntimeError):
        await h.run()

    assert closed.state == "open"
    assert closed.failure_reason == "Error Type:RuntimeError Error message: boom"
    assert closed.retry_after == NOW + timedelta(minutes=60)


async def test_too_few_recent_failures_leave_the_breaker_closed(harness):
    closed = breaker("closed", threshold=3)
    h = harness([step(1, "boom")], closed, failed_runs=2)

    with pytest.raises(RuntimeError):
        await h.run()

    assert closed.state == "closed"


async def test_one_failure_while_half_open_reopens_the_breaker_straight_away(harness):
    half_open = breaker("half-open", threshold=3)
    h = harness([step(1, "boom")], half_open, failed_runs=0)

    with pytest.raises(RuntimeError):
        await h.run()

    assert half_open.state == "open"
    assert half_open.retry_after == NOW + timedelta(minutes=60)


async def test_a_success_while_half_open_closes_the_breaker_and_clears_its_reason(harness):
    half_open = breaker("half-open")

    await harness([step(1, "ok")], half_open).run()

    assert half_open.state == "closed"
    assert half_open.failure_reason is None and half_open.retry_after is None


async def test_an_open_breaker_blocks_a_queued_run_before_any_step_runs(harness):
    h = harness([step(1, "ok")], breaker("open"), callback_url="https://hook")

    result = await h.run()

    assert result is h.database.run
    run = h.database.run
    assert (run.status, run.error_type, run.error_message) == (
        "blocked", "CircuitBreakerOpen", "Pipeline circuit breaker is open",
    )
    assert run.ended_at == NOW
    assert h.calls == []


async def test_a_blocked_run_does_not_trigger_the_agent_the_run_record_or_the_webhook(harness):
    h = harness([step(1, "ok")], breaker("open"), callback_url="https://hook")

    await h.run()

    assert h.recorded == [] and h.webhooks == []
    h.agent.assert_not_awaited()


async def test_an_open_breaker_does_not_overwrite_a_run_that_is_no_longer_queued(harness):
    h = harness([step(1, "ok")], breaker("open"), run_status="running")

    with pytest.raises(InvalidPipelineRunStatusError, match="'running'"):
        await h.run()

    assert h.database.run.status == "running"


# ---- an open breaker whose wait is over ----


async def test_an_open_breaker_whose_wait_is_over_goes_half_open_and_lets_the_run_through(harness):
    due = breaker("open", retry_after=NOW - timedelta(seconds=1))
    h = harness([step(1, "ok")], due)

    await h.run()

    assert h.calls == ["ok"] and h.database.run.status == "success"
    assert due.state == "closed"  # it went half-open for this run, and the success closed it
    assert due.failure_reason is None and due.retry_after is None


async def test_the_wait_ending_exactly_now_counts_as_over(harness):
    due = breaker("open", retry_after=NOW)

    await harness([step(1, "ok")], due).run()

    assert due.state == "closed"


async def test_the_trial_run_leaves_the_breaker_half_open_while_it_runs(harness):
    due = breaker("open", retry_after=NOW - timedelta(minutes=1))
    h = harness([step(1, "ok")], due)
    seen = []

    async def look(config, context):
        seen.append(due.state)

    executor.step_registry["look"] = look
    try:
        h.database.steps = [step(1, "look")]
        await h.run()
    finally:
        del executor.step_registry["look"]

    assert seen == ["half-open"]
    assert due.updated_at == NOW


async def test_a_failed_trial_run_opens_the_breaker_again_for_a_full_wait(harness):
    due = breaker("open", threshold=3, window=60, retry_after=NOW - timedelta(minutes=1))
    h = harness([step(1, "boom")], due)

    with pytest.raises(RuntimeError):
        await h.run()

    assert due.state == "open"
    assert due.retry_after == NOW + timedelta(minutes=60)


async def test_an_open_breaker_that_is_still_waiting_blocks_the_run(harness):
    waiting = breaker("open", retry_after=NOW + timedelta(seconds=1))
    h = harness([step(1, "ok")], waiting)

    await h.run()

    assert h.database.run.status == "blocked" and h.calls == []
    assert waiting.state == "open"


async def test_a_breaker_opened_by_hand_without_a_retry_time_stays_open(harness):
    by_hand = breaker("open", retry_after=None)
    h = harness([step(1, "ok")], by_hand)

    await h.run()

    assert h.database.run.status == "blocked" and by_hand.state == "open"


async def test_when_another_worker_made_it_half_open_first_this_run_goes_ahead_too(harness):
    due = breaker("open", retry_after=NOW - timedelta(seconds=1))
    h = harness([step(1, "ok")], due)
    h.database.another_worker_first = True

    await h.run()

    assert h.database.run.status == "success" and h.calls == ["ok"]


# ---- a worker that lost its lease must not write ----


def take_over(h, monkeypatch, then_fail=False, name="taken_over"):
    """A step that plays another worker claiming the run in the middle of this worker's step."""

    async def step_fn(config, context):
        h.calls.append(name)
        h.database.run.attempts += 1
        if then_fail:
            raise RuntimeError("late failure")

    monkeypatch.setitem(executor.step_registry, name, step_fn)


async def test_a_worker_whose_run_was_taken_over_does_not_mark_it_a_success(harness, monkeypatch):
    h = harness([step(1, "ok")])
    take_over(h, monkeypatch)
    h.database.steps = [step(1, "taken_over")]

    result = await h.run()  # no error: the worker just stops

    assert result is h.database.run
    assert h.database.run.status == "running"  # still the other worker's to finish
    assert h.database.run.ended_at is None
    assert h.recorded == [] and h.webhooks == []
    h.agent.assert_not_awaited()


async def test_a_worker_whose_run_was_taken_over_does_not_fail_it_or_trip_the_breaker(harness, monkeypatch):
    closed = breaker("closed", threshold=1)
    h = harness([step(1, "ok")], closed, failed_runs=99)  # one more failure would open the breaker
    take_over(h, monkeypatch, then_fail=True)
    h.database.steps = [step(1, "taken_over")]

    await h.run()  # the late failure is not raised either

    assert h.database.run.status == "running" and h.database.run.error_type is None
    assert closed.state == "closed"
    assert h.recorded == [] and h.webhooks == []
    h.agent.assert_not_awaited()


async def test_a_worker_whose_run_was_taken_over_stops_instead_of_retrying(harness, monkeypatch):
    h = harness([step(1, "ok")])
    take_over(h, monkeypatch, then_fail=True)
    h.database.steps = [step(1, "taken_over", retry={"max_retries": 3, "base_delay": 0})]

    await h.run()

    assert h.calls == ["taken_over"]  # the step was not run a second time
    assert h.database.run.status == "running"  # and not marked retrying
    assert h.database.run.retry_count == 0


async def test_a_run_the_reaper_put_back_in_the_queue_cannot_be_finished_by_the_old_worker(harness, monkeypatch):
    h = harness([step(1, "ok")])

    async def reaped(config, context):
        h.database.run.status = "queued"  # the lease ran out and the reaper took the run back

    monkeypatch.setitem(executor.step_registry, "reaped", reaped)
    h.database.steps = [step(1, "reaped")]

    await h.run()

    assert h.database.run.status == "queued"
    assert h.recorded == []


async def test_a_worker_that_learns_its_lease_is_lost_stops_before_the_next_step(harness, monkeypatch):
    h = harness([step(1, "ok")])
    keeper = FakeLeaseKeeper()
    monkeypatch.setattr(executor, "start_lease_keeper", lambda run_id, attempt: keeper)

    async def lose(config, context):
        h.calls.append("lose")
        keeper.lost = True  # the heartbeat found out

    monkeypatch.setitem(executor.step_registry, "lose", lose)
    h.database.steps = [step(1, "lose"), step(2, "ok")]

    await h.run()

    assert h.calls == ["lose"]  # the second step never ran
    assert h.database.run.status == "running"
    assert h.recorded == [] and h.webhooks == []


async def test_the_heartbeat_gets_the_attempt_number_of_the_claim_and_the_writes_use_it(harness, monkeypatch):
    h = harness([step(1, "ok")])
    h.database.run.attempts = 4  # claimed three times before
    started = []
    monkeypatch.setattr(executor, "start_lease_keeper", lambda run_id, attempt: started.append((run_id, attempt)) or FakeLeaseKeeper())

    await h.run()

    assert started == [(1, 5)]
    assert h.database.run.status == "success" and h.database.run.attempts == 5
    assert h.recorded[0]["attempt"] == 5


# ---- a run that cannot be claimed ----


async def test_a_run_that_is_not_queued_is_refused_and_left_as_it_was(harness):
    h = harness([step(1, "ok")], run_status="success")

    with pytest.raises(InvalidPipelineRunStatusError, match="expected 'queued'"):
        await h.run()

    assert h.database.run.status == "success"
    assert h.calls == []


async def test_a_run_id_that_does_not_exist_is_a_value_error(harness):
    h = harness([step(1, "ok")])
    h.database.run = None

    with pytest.raises(ValueError, match="not found"):
        await h.run()


# ---- what must never break a run ----


async def test_a_failure_writing_the_run_record_does_not_change_the_result(harness, monkeypatch):
    h = harness([step(1, "ok")])

    async def broken_record(context):
        raise ObservabilityRecordingError("disk full")

    monkeypatch.setattr(executor, "run_observability_recording", broken_record)

    result = await h.run()

    assert result.status == "success"


async def test_a_failing_diagnostic_agent_does_not_change_the_result_or_stop_the_webhook(harness):
    h = harness([step(1, "ok")], callback_url="https://hook")
    h.agent.side_effect = RuntimeError("model is down")

    result = await h.run()

    assert result.status == "success"
    assert h.webhooks == [("https://hook", 1, 7, 3, "success", None)]


async def test_the_agent_runs_after_a_failure_and_its_recommendation_goes_to_the_webhook(harness):
    h = harness([step(1, "boom")], callback_url="https://hook")

    with pytest.raises(RuntimeError):
        await h.run()

    h.agent.assert_awaited_once()
    assert h.agent.await_args.args[0]["run_status"] == "failed"
    assert h.webhooks == [("https://hook", 1, 7, 3, "failed", 99)]


async def test_a_pipeline_with_no_callback_url_sends_no_webhook(harness):
    h = harness([step(1, "ok")], callback_url=None)

    await h.run()

    assert h.webhooks == []
