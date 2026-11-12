"""The real worker program: killed in the middle of a run with a second one taking the run over, and starting runs
from a schedule.

This is the failure that used to leave a run in 'running' for ever. It uses the real `python -m worker.app.main`, the
real queue and the real database, with short timings so the test takes a few seconds, not minutes.
"""
import asyncio
import os
import signal
import socket
import subprocess
import sys
from pathlib import Path

import pytest

from shared import run_queue

from tests.integration import helpers

REPO_ROOT = Path(__file__).resolve().parents[2]


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def start_worker(tmp_path, name):
    env = {
        **os.environ,
        "RUN_LEASE_SECONDS": "6",
        "REAPER_INTERVAL_SECONDS": "1",
        "SCHEDULER_INTERVAL_SECONDS": "1",
        "ENQUEUE_VISIBILITY_SECONDS": "5",
        "WORKER_METRICS_PORT": str(free_port()),
        "OTEL_TRACES_ENABLED": "false",
        "ALLOW_PRIVATE_URLS": "true",  # the data source of these tests is on 127.0.0.1
        "PYTHONPATH": str(REPO_ROOT),
    }
    log = open(tmp_path / f"{name}.log", "w")
    # a new process group on Windows, so Ctrl+Break can be sent to this worker alone
    flags = subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0
    return subprocess.Popen(
        [sys.executable, "-m", "worker.app.main"], cwd=tmp_path, env=env, stdout=log, stderr=log, creationflags=flags
    )


def ask_to_stop(worker):
    """What Kubernetes does first: SIGTERM (Ctrl+Break on Windows, the nearest thing), then it waits."""
    worker.send_signal(signal.CTRL_BREAK_EVENT if os.name == "nt" else signal.SIGTERM)


async def wait_for(run_id, statuses, seconds):
    for _ in range(int(seconds * 4)):
        run = await helpers.get_run(run_id)
        if run.status in statuses:
            return run
        await asyncio.sleep(0.25)
    return await helpers.get_run(run_id)


@pytest.mark.real_http
async def test_a_run_survives_its_worker_being_killed(world, tmp_path):
    pipeline = await world.pipeline("/slow.csv?secs=4")
    run_id = await world.run(pipeline)

    first = start_worker(tmp_path, "worker1")
    try:
        await run_queue.enqueue_run(run_id)
        run = await wait_for(run_id, {"running"}, 120)
        assert run.status == "running" and run.attempts == 1
        first.kill()  # no warning, no cleanup, as if the machine had gone
        first.wait(timeout=10)
    finally:
        if first.poll() is None:
            first.kill()

    second = start_worker(tmp_path, "worker2")
    try:
        run = await wait_for(run_id, {"success", "failed"}, 150)
    finally:
        second.kill()
        second.wait(timeout=10)

    assert run.status == "success"
    assert run.attempts == 2  # claimed by the first worker, then taken over by the second


@pytest.mark.real_http
async def test_a_worker_starts_the_runs_of_a_schedule_and_finishes_them(world, tmp_path):
    pipeline = await world.pipeline("/ok.csv")
    schedule_id = await helpers.make_schedule(world.tenant_id, pipeline, "* * * * *")
    await helpers.set_schedule(schedule_id, handled_until="sql:now() - interval '3 hours'")

    worker = start_worker(tmp_path, "worker")
    try:
        for _ in range(240):  # up to a minute
            runs = await helpers.runs_of(pipeline)
            if runs and runs[0].status in ("success", "failed"):
                break
            await asyncio.sleep(0.25)
    finally:
        worker.kill()
        worker.wait(timeout=10)

    assert [run.status for run in runs] == ["success"]  # one run for the missed times, and it finished
    assert runs[0].idempotency_key.startswith(f"schedule:{schedule_id}:")


@pytest.mark.real_http
async def test_a_worker_told_to_stop_finishes_its_run_and_takes_no_other(world, tmp_path):
    pipeline = await world.pipeline("/slow.csv?secs=4")
    other_pipeline = await world.pipeline("/ok.csv")
    run_id = await world.run(pipeline)
    waiting_id = await world.run(other_pipeline)

    worker = start_worker(tmp_path, "worker")
    try:
        await run_queue.enqueue_run(run_id)
        run = await wait_for(run_id, {"running"}, 120)
        assert run.status == "running"
        await run_queue.enqueue_run(waiting_id)  # on the queue behind the run in hand
        ask_to_stop(worker)
        exit_code = await asyncio.get_running_loop().run_in_executor(None, lambda: worker.wait(timeout=60))
    finally:
        if worker.poll() is None:
            worker.kill()

    run = await helpers.get_run(run_id)
    waiting = await helpers.get_run(waiting_id)
    assert exit_code == 0  # it left by itself, it was not killed
    assert run.status == "success" and run.attempts == 1  # the run in hand was finished, not abandoned
    assert waiting.status == "queued"  # and the next one was left for another worker
    assert waiting_id in await helpers.queue_contents()
