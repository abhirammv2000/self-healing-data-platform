"""Runs one pipeline run: claim it, execute its steps with retries, and record what happened.

The order of events:

  1. A run that does not exist is refused. If the pipeline's circuit breaker is open, the run is blocked.
  2. The run is claimed (queued -> running), so two workers can never take the same run. The claim also gives the
     run a lease that the worker keeps extending while it works. If the worker stops, the lease runs out and the
     reaper (worker/app/reaper.py) takes the run back.
  3. Each step runs in order. A failed step is retried with backoff if its config allows it.
  4. On success the run is marked done and a half-open circuit breaker is closed. (An open breaker whose retry
     time has passed becomes half-open when the next run arrives, and that run is the trial.)
  5. On failure the run is marked failed, the circuit breaker is updated, and the original error is raised.
  6. Either way, the run is saved to disk, the diagnostic agent looks at a failure, and the webhook is sent.
     None of those can change the outcome of the run.
"""
import asyncio
import json
import random
from datetime import timedelta
from pathlib import Path

from sqlalchemy import func, select, update
from sqlalchemy.exc import IntegrityError, SQLAlchemyError

from control_plane.app.models.pipeline_circuit_breakers import PipelineCircuitBreaker
from control_plane.app.models.pipeline_runs import PipelineRun
from control_plane.app.models.pipeline_steps import PipelineStep
from control_plane.app.models.pipelines import Pipeline
from shared import metrics
from shared.config import RUN_LEASE_SECONDS
from shared.db import async_session
from shared.observability import get_logger, get_tracer
from shared.utils import now_naive
from shared.webhook_dispatcher import dispatch_webhook_callback
from worker.app.exceptions import LeaseLostError, PipelineBusyError, InvalidPipelineRunStatusError, ObservabilityRecordingError, UnknownStepTypeError
from shared.run_queue import wake_waiting_run
from worker.app.lease import start_lease_keeper
from worker.app.step_handlers import step_registry

log = get_logger(__name__)
tracer = get_tracer(__name__)


async def run_executor_service(run_id: int):
    # a thin wrapper that puts the whole run in one trace span
    with tracer.start_as_current_span("execute_pipeline_run", attributes={"run_id": run_id}):
        return await _run_executor_service(run_id)


async def _run_executor_service(run_id: int):
    async with async_session() as session:
        try:
            pipeline_run = await _fetch_run(session, run_id)
            if not pipeline_run:
                raise ValueError(f"Pipeline run {run_id} not found")
            pipeline_id = pipeline_run.pipeline_id

            circuit_breaker = await _fetch_circuit_breaker(session, pipeline_id)
            if circuit_breaker and circuit_breaker.state == "open":
                # an open breaker whose wait is over lets this run through as the trial (it becomes half-open)
                if not await _half_open_if_due(session, circuit_breaker):
                    # nothing below this point runs for a blocked run: no steps, no agent, no webhook
                    return await _block_run(session, run_id, pipeline_id)

            try:
                attempt = await _claim_run(session, run_id)
            except PipelineBusyError:
                # another run of this pipeline is running: this one waits its turn and is put back on the queue later
                return await _defer_run(session, run_id, pipeline_id)
        except SQLAlchemyError:
            await session.rollback()
            raise

        if attempt is None:
            await _raise_run_not_claimable(session, run_id)

        # re-fetch the run now that it is marked running
        pipeline_run = await _fetch_run(session, run_id)
        if not pipeline_run:
            raise ValueError(f"Pipeline run {run_id} not found")

        # `attempt` is this worker's claim number, and the fencing token: every write to the run below checks that the
        # run is still on this attempt, so a worker that lost its lease cannot overwrite the worker that took over
        lease_keeper = start_lease_keeper(run_id, attempt)
        lease_lost = False
        try:
            # this dictionary is passed from step to step: earlier steps write values, later steps read them
            run_context = {
                "run_id": run_id,
                "pipeline_id": pipeline_run.pipeline_id,
                "tenant_id": pipeline_run.tenant_id,
                "retry_count": 0,
                "attempt": attempt,
            }

            for step in await _fetch_steps(session, pipeline_run.pipeline_id):
                if lease_keeper.lost:
                    raise LeaseLostError(f"run {run_id} was taken over by another worker")
                await _run_step_with_retries(session, pipeline_run, step, run_context, attempt)

            await _mark_run_succeeded(session, run_id, pipeline_id, pipeline_run, run_context, attempt)
            await _close_half_open_circuit_breaker(session, pipeline_id)
            return pipeline_run

        except LeaseLostError as error:
            # another worker owns the run now and will report how it ended. This one writes nothing more:
            # no failure, no breaker change, no agent, no webhook
            lease_lost = True
            log.warning("run_lease_lost", run_id=run_id, attempt=attempt, detail=str(error))
            return pipeline_run

        except Exception as error:
            # the run has to be fetched again after the rollback: an expired object cannot reload itself in async code
            try:
                pipeline_run = await _record_failure(session, run_id, pipeline_id, pipeline_run, run_context, error, attempt)
            except LeaseLostError as lost_error:
                lease_lost = True
                log.warning("run_lease_lost", run_id=run_id, attempt=attempt, detail=str(lost_error))
                return pipeline_run
            await _update_circuit_breaker_after_failure(session, pipeline_id, error)
            raise  # the original error goes to the caller

        finally:
            await lease_keeper.stop()
            if not lease_lost:
                # the pipeline is free as soon as the run has ended, so the next waiting run is offered before the
                # slow part (the agent and the webhook) rather than after it
                await wake_waiting_run(pipeline_id)
                await _finish_run(session, run_id, pipeline_run, run_context)


# ---- getting ready to run ----


async def _fetch_run(session, run_id):
    result = await session.execute(select(PipelineRun).where(PipelineRun.id == run_id))
    return result.scalar_one_or_none()


async def _fetch_circuit_breaker(session, pipeline_id):
    result = await session.execute(
        select(PipelineCircuitBreaker).where(PipelineCircuitBreaker.pipeline_id == pipeline_id)
    )
    return result.scalar_one_or_none()


async def _fetch_steps(session, pipeline_id):
    result = await session.execute(
        select(PipelineStep).where(PipelineStep.pipeline_id == pipeline_id).order_by(PipelineStep.step_order)
    )
    return result.scalars().all()


async def _half_open_if_due(session, circuit_breaker) -> bool:
    """Move an open breaker to half-open once its retry_after time has passed. True means the run may go ahead.

    A breaker with no retry_after was opened by hand and stays open until a person changes it. The change is one
    conditional UPDATE, so when several workers get here together exactly one makes it, and the others see the
    breaker already half-open and go ahead too. While it is half-open every run is let through, and the first
    to finish decides: a success closes the breaker and a failure opens it again for another full wait.
    """
    now = now_naive()
    if circuit_breaker.retry_after is None or circuit_breaker.retry_after > now:
        return False
    pipeline_id = circuit_breaker.pipeline_id
    result = await session.execute(
        update(PipelineCircuitBreaker)
        .where(
            PipelineCircuitBreaker.pipeline_id == pipeline_id,
            PipelineCircuitBreaker.state == "open",
            PipelineCircuitBreaker.retry_after <= now,
        )
        .values(state="half-open", updated_at=now)
    )
    await session.commit()
    # the session does not expire objects on commit, so reload the breaker to show what the database says now
    await session.refresh(circuit_breaker)
    if result.rowcount == 1:
        metrics.circuit_breaker_transitions_total.labels(to_state="half-open").inc()
        log.info("circuit_breaker_half_open", pipeline_id=pipeline_id)
    return circuit_breaker.state != "open"


async def _block_run(session, run_id, pipeline_id):
    """Mark a queued run as blocked because the circuit breaker is open, and return it."""
    now = now_naive()
    # only a queued run is blocked, so a run that is already running, done or failed is never overwritten
    result = await session.execute(
        update(PipelineRun)
        .where(PipelineRun.id == run_id, PipelineRun.status == "queued")
        .values(
            status="blocked",
            ended_at=now,
            updated_at=now,
            error_type="CircuitBreakerOpen",
            error_message="Pipeline circuit breaker is open",
        )
    )
    await session.commit()

    if result.rowcount == 0:
        await _raise_run_not_claimable(session, run_id)

    pipeline_run = await _fetch_run(session, run_id)
    log.warning("pipeline_run_blocked", run_id=run_id, pipeline_id=pipeline_id, reason="circuit_breaker_open")
    return pipeline_run


async def _claim_run(session, run_id):
    """Change the run from queued to running in one statement. Returns the new attempt number, or None when the run
    was not queued (someone else has it, or it is finished). The attempt number is the fencing token of this claim.

    This is one UPDATE and not a SELECT followed by an UPDATE. With a SELECT first, two workers could both see
    'queued' and both run it.
    """
    now = now_naive()
    try:
        result = await session.execute(
            update(PipelineRun)
            .where(PipelineRun.id == run_id, PipelineRun.status == "queued")
            .values(
                status="running",
                started_at=now,
                updated_at=now,
                attempts=PipelineRun.attempts + 1,
                lease_expires_at=func.now() + timedelta(seconds=RUN_LEASE_SECONDS),
            )
            .returning(PipelineRun.attempts)
        )
        await session.commit()
    except IntegrityError as error:
        await session.rollback()
        # the database allows one active run per pipeline (uq_pipeline_runs_one_active), and another one has it
        if "uq_pipeline_runs_one_active" in str(error.orig):
            raise PipelineBusyError(f"another run of the pipeline of run {run_id} is running") from None
        raise
    return result.scalar_one_or_none()


async def _defer_run(session, run_id, pipeline_id):
    """Leave a run queued because its pipeline already has a run going, and return the run.

    Its id was taken off the queue, so it is marked as put on the queue just now. The reaper puts it back after
    ENQUEUE_VISIBILITY_SECONDS, which also keeps a run that waits a long time from being tried again and again.
    """
    await session.execute(
        update(PipelineRun)
        .where(PipelineRun.id == run_id, PipelineRun.status == "queued")
        .values(last_enqueued_at=func.now(), updated_at=now_naive())
    )
    await session.commit()
    metrics.pipeline_runs_deferred_total.inc()
    log.info("pipeline_run_deferred", run_id=run_id, pipeline_id=pipeline_id, reason="pipeline_busy")
    return await _fetch_run(session, run_id)


async def _raise_run_not_claimable(session, run_id):
    """The run could not be claimed or blocked: either it does not exist or it is no longer queued."""
    pipeline_run = await _fetch_run(session, run_id)
    if not pipeline_run:
        raise ValueError(f"Pipeline run {run_id} not found")
    raise InvalidPipelineRunStatusError(f"Pipeline run {run_id} is in '{pipeline_run.status}' state, expected 'queued'")


# ---- running the steps ----


async def _run_step_with_retries(session, pipeline_run, step, run_context, claim_attempt):
    run_id = run_context["run_id"]
    step_type = step.step_type
    config = step.config or {}
    log.info("pipeline_step_started", run_id=run_id, step_order=step.step_order, step_type=step_type, config=config)

    with tracer.start_as_current_span(
        "pipeline_step", attributes={"run_id": run_id, "step_order": step.step_order, "step_type": step_type}
    ):
        perform_step = step_registry.get(step_type)
        if not perform_step:
            raise UnknownStepTypeError(f"Unknown step type: {step_type}")

        # a step with no retry config gets one attempt
        retry_config = config.get("retry", {})
        max_retries = retry_config.get("max_retries", 0)
        strategy = retry_config.get("strategy", None)
        base_delay = retry_config.get("base_delay", 0)
        max_attempts = max_retries + 1  # the first attempt plus the retries

        for attempt in range(1, max_attempts + 1):
            try:
                await perform_step(step.config, run_context)
                _record_attempt(run_context, step, attempt, "success", error=None, delay=0)
                break

            except Exception as error:
                log.warning(
                    "pipeline_step_attempt_failed",
                    run_id=run_id,
                    step_type=step_type,
                    attempt=attempt,
                    max_attempts=max_attempts,
                    error=str(error),
                )
                is_final_attempt = attempt == max_attempts
                delay = 0 if is_final_attempt else calculate_delay(strategy, base_delay, attempt)

                # the attempt is recorded before the final-attempt check, so the last failure is recorded too
                _record_attempt(run_context, step, attempt, "failed", error=str(error), delay=delay)
                if is_final_attempt:
                    raise  # no retries left: the run fails

                run_context["retry_count"] = run_context.get("retry_count", 0) + 1
                await _save_retry(session, pipeline_run, run_id, claim_attempt)
                await asyncio.sleep(delay)


def _record_attempt(run_context, step, attempt, status, error, delay):
    run_context.setdefault("step_attempts", []).append(
        {
            "step_order": step.step_order,
            "step_type": step.step_type,
            "attempt": attempt,
            "status": status,
            "error": error,
            "delay": delay,
        }
    )


async def _save_retry(session, pipeline_run, run_id, attempt):
    """Count the retry on the run and mark it as retrying. A failure here is logged and does not stop the retry,
    except that a run taken over by another worker stops this one."""
    try:
        await _fence(session, run_id, attempt)
        pipeline_run.retry_count += 1
        pipeline_run.updated_at = now_naive()
        pipeline_run.status = "retrying"
        await session.commit()
        await session.refresh(pipeline_run)
    except LeaseLostError:
        raise
    except Exception as error:
        await session.rollback()
        log.error("retry_count_update_failed", run_id=run_id, error=str(error))


# ---- how a run ends ----


async def _mark_run_succeeded(session, run_id, pipeline_id, pipeline_run, run_context, attempt):
    await _fence(session, run_id, attempt)
    now = now_naive()
    pipeline_run.status = "success"
    pipeline_run.ended_at = now
    pipeline_run.updated_at = now
    await session.commit()
    await session.refresh(pipeline_run)

    run_context["run_status"] = "success"
    metrics.pipeline_runs_total.labels(status="success").inc()
    if pipeline_run.started_at:
        metrics.pipeline_run_duration_seconds.observe((now - pipeline_run.started_at).total_seconds())
    log.info("pipeline_run_succeeded", run_id=run_id, pipeline_id=pipeline_id)


async def _close_half_open_circuit_breaker(session, pipeline_id):
    """A success while the breaker is half-open proves the pipeline works again, so the breaker closes."""
    try:
        circuit_breaker = await _fetch_circuit_breaker(session, pipeline_id)
        if circuit_breaker and circuit_breaker.state == "half-open":
            circuit_breaker.state = "closed"
            # a closed breaker means no recent failure, so the failure details are cleared
            circuit_breaker.failure_reason = None
            circuit_breaker.retry_after = None
            circuit_breaker.updated_at = now_naive()
            await session.commit()
            await session.refresh(circuit_breaker)
            metrics.circuit_breaker_transitions_total.labels(to_state="closed").inc()
            log.info("circuit_breaker_closed", pipeline_id=pipeline_id)
    except Exception as error:
        await session.rollback()
        log.error("circuit_breaker_update_failed", pipeline_id=pipeline_id, error=str(error))


async def _record_failure(session, run_id, pipeline_id, pipeline_run, run_context, error, attempt):
    """Mark the run as failed and return the run to use from here on. Raises LeaseLostError, and writes nothing,
    when another worker has taken the run over."""
    await session.rollback()
    run_context["run_status"] = "failed"
    run_context["error_type"] = type(error).__name__
    run_context["error_message"] = str(error)

    counted = False
    try:
        await _fence(session, run_id, attempt)
        metrics.pipeline_runs_total.labels(status="failed").inc()
        counted = True
        pipeline_run = await _fetch_run(session, run_id)
        if pipeline_run:
            now = now_naive()
            pipeline_run.status = "failed"
            pipeline_run.ended_at = now
            pipeline_run.updated_at = now
            pipeline_run.error_type = type(error).__name__
            pipeline_run.error_message = str(error)
            await session.commit()
            await session.refresh(pipeline_run)
            if pipeline_run.started_at:
                metrics.pipeline_run_duration_seconds.observe((now - pipeline_run.started_at).total_seconds())
            log.error(
                "pipeline_run_failed",
                run_id=run_id,
                pipeline_id=pipeline_id,
                error_type=type(error).__name__,
                error_message=str(error),
            )
    except LeaseLostError:
        raise
    except Exception as save_error:
        # log it and carry on, so the original error is not hidden
        if not counted:
            metrics.pipeline_runs_total.labels(status="failed").inc()
        await session.rollback()
        log.error("run_status_update_failed", run_id=run_id, error=str(save_error))

    return pipeline_run


async def _fence(session, run_id, attempt):
    """Check that this worker still owns the run, and keep the row locked until the caller's next commit.

    The run is owned by whichever worker made its latest claim, and that claim's number is `attempt`. If the lease
    ran out and the reaper gave the run to another worker, the row now has a higher attempt number (or is not running
    any more), and this worker must not write to it. Locking the row makes the check and the write one step: the
    reaper cannot hand the run over in between. Raises LeaseLostError, after releasing the lock, when it is not ours.
    """
    row = (
        await session.execute(
            select(PipelineRun.attempts, PipelineRun.status).where(PipelineRun.id == run_id).with_for_update()
        )
    ).one_or_none()
    if row is None or row.attempts != attempt or row.status not in ("running", "retrying"):
        await session.rollback()
        found = "gone" if row is None else f"attempt {row.attempts}, {row.status}"
        raise LeaseLostError(f"run {run_id} is no longer attempt {attempt} of this worker ({found})")


async def _update_circuit_breaker_after_failure(session, pipeline_id, error):
    try:
        circuit_breaker = await _fetch_circuit_breaker(session, pipeline_id)
        if not circuit_breaker:
            return

        now = now_naive()
        window_minutes = circuit_breaker.failure_window_minutes
        failure_reason = f"Error Type:{type(error).__name__} Error message: {str(error)}"
        retry_after = now + timedelta(minutes=window_minutes)

        if circuit_breaker.state == "half-open":
            # the one trial run failed, so the breaker opens again straight away
            mark_circuit_breaker_open(circuit_breaker, failure_reason, retry_after)
            await session.commit()
            await session.refresh(circuit_breaker)
            metrics.circuit_breaker_transitions_total.labels(to_state="open").inc()
            log.warning("circuit_breaker_opened", pipeline_id=pipeline_id, failure_reason=failure_reason)

        elif circuit_breaker.state == "closed":
            # the breaker opens once enough runs have failed inside the window
            window_start = now - timedelta(minutes=window_minutes)
            result = await session.execute(
                select(func.count())
                .select_from(PipelineRun)
                .where(
                    PipelineRun.pipeline_id == pipeline_id,
                    PipelineRun.status == "failed",
                    PipelineRun.ended_at >= window_start,
                )
            )
            failed_runs_count = result.scalar_one()

            if failed_runs_count >= circuit_breaker.failure_count_threshold:
                mark_circuit_breaker_open(circuit_breaker, failure_reason, retry_after)
                await session.commit()
                await session.refresh(circuit_breaker)
                metrics.circuit_breaker_transitions_total.labels(to_state="open").inc()
                log.warning(
                    "circuit_breaker_opened",
                    pipeline_id=pipeline_id,
                    failure_reason=failure_reason,
                    failed_runs_count=failed_runs_count,
                )
        # an open breaker is left alone: another run may have opened it while this one was running,
        # so opening it again would be wasted work

    except Exception as update_error:
        await session.rollback()
        log.error("circuit_breaker_update_failed", pipeline_id=pipeline_id, error=str(update_error))


async def _finish_run(session, run_id, pipeline_run, run_context):
    """What happens after every run, whatever the outcome. Nothing here can change the outcome."""
    try:
        await run_observability_recording(run_context)
    except ObservabilityRecordingError as error:
        log.error("observability_recording_failed", run_id=run_id, error=str(error))

    # The agent is awaited, unlike the webhook, because the webhook needs the recommendation id for its link.
    # The agent catches its own errors and returns None, so this cannot break the run.
    recommendation_id = None
    try:
        log.info("diagnostic_layer_started", run_id=run_id)
        # imported here so a broken agent dependency cannot stop the worker from starting
        from worker.app.agent.diagnostic_agent import run_diagnostic_agent

        recommendation_id = await run_diagnostic_agent(run_context)
    except Exception as error:
        log.error("diagnostic_agent_failed", run_id=run_id, error=str(error))

    try:
        result = await session.execute(select(Pipeline).where(Pipeline.id == pipeline_run.pipeline_id))
        pipeline = result.scalar_one_or_none()
        if pipeline and pipeline.callback_url:
            # fire and forget: the run does not wait for the callback
            asyncio.create_task(
                dispatch_webhook_callback(
                    pipeline.callback_url,
                    run_id,
                    pipeline_run.pipeline_id,
                    pipeline_run.tenant_id,
                    run_context["run_status"],
                    recommendation_id,
                )
            )
    except SQLAlchemyError:
        await session.rollback()
        log.error("callback_url_fetch_failed", run_id=run_id)


async def run_observability_recording(run_context):
    log.info("observability_recording_started", run_id=run_context.get("run_id"))

    run_id = run_context["run_id"]
    # saved in a local folder for now
    file_path = Path("data") / "runs" / str(run_id) / "run_context.json"
    file_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        with open(file_path, "w") as f:
            json.dump(run_context, f, indent=4)
    except Exception as error:
        raise ObservabilityRecordingError(f"Failed to write {str(file_path)}: {error}")
    log.info("observability_recording_succeeded", run_id=run_id)


# ---- helpers ----


def calculate_delay(strategy, base_delay, attempt):
    """How long to wait before the next attempt."""
    if strategy == "exponential_backoff":
        log.info("retry_strategy", strategy="exponential_backoff")
        return base_delay * (2 ** (attempt - 1))
    elif strategy == "exponential_backoff_jitter":
        log.info("retry_strategy", strategy="exponential_backoff_jitter")
        calculated_delay = base_delay * (2 ** (attempt - 1))
        return calculated_delay + random.uniform(0.5 * calculated_delay, calculated_delay)
    else:
        log.info("retry_strategy", strategy="none")
        return base_delay


def mark_circuit_breaker_open(circuit_breaker, failure_reason, retry_after):
    circuit_breaker.state = "open"
    circuit_breaker.failure_reason = failure_reason
    circuit_breaker.retry_after = retry_after
    circuit_breaker.updated_at = now_naive()
