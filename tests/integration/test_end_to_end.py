"""The whole platform, as a user sees it: the real API and the real worker as two separate programs, a real Postgres and
Redis, and a data source and a webhook receiver on real sockets. Everything goes through HTTP.

What is checked: a tenant is created and authenticated, a four-step pipeline is built and run, the data lands in the
warehouse, the webhook arrives once, a repeated request does not make a second run, a failing source fails the run with
a reason, a source or webhook on a private address is refused, and both programs refuse to start with a bad
configuration.

Not covered here: the diagnostic agent (it needs a model), and anything that needs a cluster.
"""
import asyncio
import http.server
import json
import os
import socket
import subprocess
import sys
import threading
import uuid
from pathlib import Path

import httpx
import pytest
from sqlalchemy import text

from shared.db import dw_engine

REPO_ROOT = Path(__file__).resolve().parents[2]
ADMIN = {"X-Admin-Secret": os.environ.get("ADMIN_SECRET_KEY", "")}
CSV = "id,value,note\n1,10,a\n2,20,b\n3,30,c\n4,40,d\n"

pytestmark = pytest.mark.real_http


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class Receiver:
    """A webhook receiver and a data source in one local server."""

    def __init__(self):
        self.webhooks = []
        outer = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                if self.path.startswith("/fail"):
                    self.send_response(500)
                    self.end_headers()
                    return
                body = CSV.encode()
                self.send_response(200)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_POST(self):
                length = int(self.headers.get("Content-Length", 0))
                outer.webhooks.append(json.loads(self.rfile.read(length) or b"{}"))
                self.send_response(204)
                self.end_headers()

            def log_message(self, *args):
                pass

        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}"

    def stop(self):
        self.server.shutdown()


@pytest.fixture
def receiver():
    server = Receiver()
    yield server
    server.stop()


class Stack:
    def __init__(self, api, base_url, processes):
        self.api = api
        self.base_url = base_url
        self.processes = processes


def _env(tmp_path, allow_private, **overrides):
    env = {
        **os.environ,
        "OTEL_TRACES_ENABLED": "false",
        "ALLOW_PRIVATE_URLS": "true" if allow_private else "false",
        "WORKER_METRICS_PORT": str(free_port()),
        "SCHEDULER_INTERVAL_SECONDS": "2",
        "REAPER_INTERVAL_SECONDS": "2",
        "DIAGNOSTIC_TIMEOUT_SECONDS": "3",  # there is no model here, so the agent is cut off quickly
        "PYTHONPATH": str(REPO_ROOT),
    }
    env.update(overrides)
    return {key: value for key, value in env.items() if value is not None}


def _spawn(args, tmp_path, name, env):
    log = open(tmp_path / f"{name}.log", "w")
    flags = subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0
    return subprocess.Popen(
        [sys.executable, *args], cwd=tmp_path, env=env, stdout=log, stderr=log, creationflags=flags
    )


async def _start_stack(tmp_path, allow_private):
    port = free_port()
    env = _env(tmp_path, allow_private)
    api = _spawn(
        ["-m", "uvicorn", "control_plane.app.main:app", "--host", "127.0.0.1", "--port", str(port)], tmp_path, "api", env
    )
    worker = _spawn(["-m", "worker.app.main"], tmp_path, "worker", env)
    base_url = f"http://127.0.0.1:{port}"
    async with httpx.AsyncClient() as client:
        for _ in range(240):  # up to a minute for both programs to import and connect
            if api.poll() is not None or worker.poll() is not None:
                break
            try:
                if (await client.get(base_url + "/ready", timeout=2)).status_code == 200:
                    return Stack(api, base_url, [api, worker])
            except httpx.HTTPError:
                pass
            await asyncio.sleep(0.25)
    for process in (api, worker):
        process.kill()
    pytest.fail("the platform did not become ready:\n" + (tmp_path / "api.log").read_text() + (tmp_path / "worker.log").read_text())


def _stop_stack(stack):
    for process in stack.processes:
        if process.poll() is None:
            process.kill()
        process.wait(timeout=10)


@pytest.fixture
async def stack(tmp_path):
    started = await _start_stack(tmp_path, allow_private=True)
    yield started
    _stop_stack(started)


@pytest.fixture
async def locked_down_stack(tmp_path):
    """The platform as it should run in production: no private addresses."""
    started = await _start_stack(tmp_path, allow_private=False)
    yield started
    _stop_stack(started)


class Client:
    """A tenant's view of the API."""

    def __init__(self, stack, tenant_id, key):
        self.stack = stack
        self.tenant_id = tenant_id
        self.headers = {"X-API-Key": key}

    @classmethod
    async def create(cls, stack):
        async with httpx.AsyncClient(base_url=stack.base_url) as http:
            response = await http.post("/tenants/", json={"name": f"e2e-{uuid.uuid4().hex[:8]}"}, headers=ADMIN)
        assert response.status_code == 201, response.text
        body = response.json()
        return cls(stack, body["id"], body["api_key"])

    async def request(self, method, path, **kwargs):
        headers = {**self.headers, **kwargs.pop("headers", {})}
        async with httpx.AsyncClient(base_url=self.stack.base_url, timeout=20) as http:
            return await http.request(method, f"/tenants/{self.tenant_id}{path}", headers=headers, **kwargs)

    async def build_pipeline(self, source_url, callback_url=None, steps=None):
        response = await self.request(
            "POST", "/pipelines/", json={"name": f"p-{uuid.uuid4().hex[:6]}", "callback_url": callback_url}
        )
        assert response.status_code == 201, response.text
        pipeline_id = response.json()["id"]
        drop_warehouse_table(self.tenant_id, pipeline_id)  # ids repeat between tests, and so would the table
        steps = steps or [
            ("ingestion", {"source_url": source_url}),
            ("validation", {"required_columns": ["id", "value"], "non_null_columns": ["id"], "min_rows": 3}),
            ("transformation", {"filter_rows": [{"column": "value", "operator": ">", "value": 10}], "drop_columns": ["note"]}),
            ("load", {}),
        ]
        for order, (step_type, config) in enumerate(steps, start=1):
            step = await self.request(
                "POST", f"/pipelines/{pipeline_id}/steps/", json={"step_type": step_type, "step_order": order, "config": config}
            )
            assert step.status_code == 201, step.text
        return pipeline_id

    async def start_run(self, pipeline_id, idempotency_key=None):
        headers = {"Idempotency-Key": idempotency_key} if idempotency_key else {}
        return await self.request("POST", f"/pipelines/{pipeline_id}/runs/", headers=headers)

    async def wait_for_callbacks(self, pipeline_id, run_id, seconds=60):
        """The webhook is sent after the diagnostic agent is done, which is after the run is already final."""
        for _ in range(int(seconds * 4)):
            callbacks = (await self.request("GET", f"/pipelines/{pipeline_id}/runs/{run_id}/callbacks/")).json()
            if callbacks:
                return callbacks
            await asyncio.sleep(0.25)
        return callbacks

    async def wait_for_run(self, pipeline_id, run_id, seconds=60):
        for _ in range(int(seconds * 4)):
            run = (await self.request("GET", f"/pipelines/{pipeline_id}/runs/{run_id}")).json()
            if run["status"] in ("success", "failed"):
                return run
            await asyncio.sleep(0.25)
        return run


def warehouse_rows(tenant_id, pipeline_id):
    with dw_engine.connect() as connection:
        return connection.execute(text(f"select id, value from tenant_{tenant_id}_pipeline_{pipeline_id} order by id")).all()


def drop_warehouse_table(tenant_id, pipeline_id):
    with dw_engine.begin() as connection:
        connection.execute(text(f"drop table if exists tenant_{tenant_id}_pipeline_{pipeline_id}"))


async def wait_until(condition, seconds=15):
    for _ in range(int(seconds * 10)):
        if condition():
            return True
        await asyncio.sleep(0.1)
    return condition()


async def test_a_pipeline_runs_from_the_api_to_the_warehouse_and_tells_its_webhook(stack, receiver):
    client = await Client.create(stack)
    pipeline_id = await client.build_pipeline(receiver.url + "/data.csv", callback_url=receiver.url + "/hook")

    created = await client.start_run(pipeline_id, idempotency_key="e2e-1")
    assert created.status_code == 201
    run_id = created.json()["id"]
    run = await client.wait_for_run(pipeline_id, run_id)

    assert run["status"] == "success", run
    assert run["error_message"] is None
    assert warehouse_rows(client.tenant_id, pipeline_id) == [(2, 20), (3, 30), (4, 40)]  # value > 10, note dropped
    assert await wait_until(lambda: len(receiver.webhooks) == 1, seconds=30)
    assert receiver.webhooks[0]["event"] == "run.completed" and receiver.webhooks[0]["run_id"] == run_id

    callbacks = (await client.request("GET", f"/pipelines/{pipeline_id}/runs/{run_id}/callbacks/")).json()
    assert [callback["status"] for callback in callbacks] == ["success"]

    # the same request again is the same run, and nothing runs twice
    repeated = await client.start_run(pipeline_id, idempotency_key="e2e-1")
    assert repeated.status_code == 200 and repeated.json()["id"] == run_id
    await asyncio.sleep(1)
    runs = (await client.request("GET", f"/pipelines/{pipeline_id}/runs/")).json()
    assert [r["id"] for r in runs] == [run_id]
    assert len(receiver.webhooks) == 1


async def test_two_runs_of_a_pipeline_one_after_the_other_both_succeed(stack, receiver):
    client = await Client.create(stack)
    pipeline_id = await client.build_pipeline(receiver.url + "/data.csv")
    first = (await client.start_run(pipeline_id)).json()["id"]
    second = (await client.start_run(pipeline_id)).json()["id"]

    assert (await client.wait_for_run(pipeline_id, first))["status"] == "success"
    assert (await client.wait_for_run(pipeline_id, second))["status"] == "success"
    assert len(warehouse_rows(client.tenant_id, pipeline_id)) == 3


async def test_a_source_that_fails_fails_the_run_with_a_reason(stack, receiver):
    client = await Client.create(stack)
    pipeline_id = await client.build_pipeline(receiver.url + "/fail", callback_url=receiver.url + "/hook")
    run_id = (await client.start_run(pipeline_id)).json()["id"]

    run = await client.wait_for_run(pipeline_id, run_id)

    assert run["status"] == "failed"
    assert "Failed to fetch" in run["error_message"] and "500" in run["error_message"]
    assert await wait_until(lambda: len(receiver.webhooks) == 1, seconds=60)
    assert receiver.webhooks[0]["event"] == "run.failed"


async def test_authentication_and_tenant_isolation(stack, receiver):
    mine = await Client.create(stack)
    other = await Client.create(stack)
    pipeline_id = await mine.build_pipeline(receiver.url + "/data.csv")

    async with httpx.AsyncClient(base_url=stack.base_url) as http:
        assert (await http.get(f"/tenants/{mine.tenant_id}/pipelines/")).status_code == 422  # no key
        wrong = await http.get(f"/tenants/{mine.tenant_id}/pipelines/", headers={"X-API-Key": "shdp_not_a_key"})
        assert wrong.status_code == 401
        # another tenant's key does not open this tenant
        crossed = await http.get(f"/tenants/{mine.tenant_id}/pipelines/", headers=other.headers)
        assert crossed.status_code == 403
        # the admin secret is not an API key, and a wrong one is refused
        assert (await http.post("/tenants/", json={"name": "x"}, headers={"X-Admin-Secret": "wrong"})).status_code == 401
        assert (await http.post("/tenants/", json={"name": "x"}, headers=mine.headers)).status_code == 422

    assert (await mine.request("GET", f"/pipelines/{pipeline_id}")).status_code == 200
    assert (await other.request("GET", f"/pipelines/{pipeline_id}")).status_code == 404


async def test_the_api_refuses_a_webhook_that_is_not_http(stack):
    client = await Client.create(stack)
    response = await client.request("POST", "/pipelines/", json={"name": "bad", "callback_url": "file:///etc/passwd"})
    assert response.status_code == 422
    assert "http" in response.text


async def test_health_ready_and_metrics(stack):
    async with httpx.AsyncClient(base_url=stack.base_url) as http:
        assert (await http.get("/health")).json() == {"status": "healthy"}
        ready = await http.get("/ready")
        assert ready.status_code == 200 and ready.json()["checks"] == {"postgres": "ok", "redis": "ok"}
        metrics = await http.get("/metrics")
        assert metrics.status_code == 200 and "http_requests_total" in metrics.text


async def test_a_source_and_a_webhook_on_a_private_address_are_refused(locked_down_stack, receiver):
    # the receiver is on 127.0.0.1, which is exactly what the platform must not be made to call
    client = await Client.create(locked_down_stack)
    pipeline_id = await client.build_pipeline(receiver.url + "/data.csv", callback_url=receiver.url + "/hook")
    run_id = (await client.start_run(pipeline_id)).json()["id"]

    run = await client.wait_for_run(pipeline_id, run_id)

    assert run["status"] == "failed"
    assert "private or internal address" in run["error_message"]
    assert warehouse_rows_missing(client.tenant_id, pipeline_id)
    callbacks = await client.wait_for_callbacks(pipeline_id, run_id)  # sent after the agent is done or cut off
    assert receiver.webhooks == []  # the webhook was not sent
    assert [callback["status"] for callback in callbacks] == ["failed"]
    assert "private or internal address" in callbacks[0]["error_message"]


def warehouse_rows_missing(tenant_id, pipeline_id):
    try:
        warehouse_rows(tenant_id, pipeline_id)
    except Exception:
        return True
    return False


def run_program(args, tmp_path, **env_overrides):
    env = _env(tmp_path, True, **env_overrides)
    process = subprocess.run(
        [sys.executable, *args], cwd=tmp_path, env=env, capture_output=True, text=True, timeout=60
    )
    return process


@pytest.mark.parametrize("name", ["ADMIN_SECRET_KEY", "API_KEY_SECRET"])
def test_the_worker_refuses_to_start_without_a_secret(tmp_path, name):
    result = run_program(["-m", "worker.app.main"], tmp_path, **{name: ""})
    assert result.returncode != 0
    assert f"{name} is not set" in result.stdout + result.stderr


@pytest.mark.parametrize("name", ["ADMIN_SECRET_KEY", "API_KEY_SECRET"])
def test_the_api_refuses_to_start_without_a_secret(tmp_path, name):
    result = run_program(
        ["-c", "import uvicorn; uvicorn.run('control_plane.app.main:app', port=%d)" % free_port()], tmp_path, **{name: ""}
    )
    assert result.returncode != 0
    assert f"{name} is not set" in result.stdout + result.stderr


def test_production_refuses_a_weak_secret(tmp_path):
    result = run_program(["-m", "worker.app.main"], tmp_path, ENV="production", API_KEY_SECRET="short")
    assert result.returncode != 0
    assert "at least 16 characters" in result.stdout + result.stderr
