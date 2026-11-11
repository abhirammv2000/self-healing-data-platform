"""The real worker program, killed in the middle of a run, and a second one that takes the run over.

This is the failure that used to leave a run in 'running' for ever. It uses the real `python -m worker.app.main`, the
real queue and the real database, with short timings so the test takes a few seconds, not minutes.
"""
import asyncio
import os
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
        "ENQUEUE_VISIBILITY_SECONDS": "5",
        "WORKER_METRICS_PORT": str(free_port()),
        "OTEL_TRACES_ENABLED": "false",
        "PYTHONPATH": str(REPO_ROOT),
    }
    log = open(tmp_path / f"{name}.log", "w")
    return subprocess.Popen([sys.executable, "-m", "worker.app.main"], cwd=tmp_path, env=env, stdout=log, stderr=log)


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
