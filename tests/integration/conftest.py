"""Fixtures for the tests that need a real Postgres and a real Redis.

They run only when SHDP_INTEGRATION=1, against the database and Redis named by DATABASE_URL, DATABASE_URL_SYNC,
DATA_WAREHOUSE_URL and REDIS_URL, after `alembic upgrade head`. They EMPTY the run queue and every table that belongs to a
tenant before each test, so use a database you do not mind losing. CI starts one for them (.github/workflows/ci.yml).
"""
import http.server
import os
import socketserver
import threading
import time
import urllib.parse

import pytest
from sqlalchemy import text

from tests.integration import helpers

ENABLED = os.getenv("SHDP_INTEGRATION") == "1"
CSV = b"id,value\n1,10\n2,20\n3,30\n"


def pytest_collection_modifyitems(config, items):
    if ENABLED:
        return
    skip = pytest.mark.skip(reason="set SHDP_INTEGRATION=1 and point the environment at a migrated Postgres and Redis")
    for item in items:
        if "tests/integration" in item.nodeid.replace("\\", "/"):
            item.add_marker(skip)


@pytest.fixture(autouse=True)
async def clean_state(tmp_path, monkeypatch):
    """A clean queue and a scratch directory for the files a run writes, and no connection kept between tests.

    The database engine and the Redis client are made once, when the code is imported, and their connections belong
    to the event loop that opened them. Every test has its own loop, so they are closed after each test.
    """
    if not ENABLED:
        yield
        return
    from shared.db import asyncio_engine
    from shared.redis_client import redis_client
    from shared.run_queue import QUEUE_NAME

    monkeypatch.chdir(tmp_path)
    async with asyncio_engine.connect() as connection:
        columns = (
            await connection.execute(
                text("select column_name from information_schema.columns where table_name='pipeline_runs'")
            )
        ).scalars().all()
    assert "lease_expires_at" in columns, "run `alembic upgrade head` on this database first"
    async with asyncio_engine.connect() as connection:
        schedule_columns = (
            await connection.execute(
                text("select column_name from information_schema.columns where table_name='schedules'")
            )
        ).scalars().all()
    assert "handled_until" in schedule_columns, "run `alembic upgrade head` on this database first"
    async with asyncio_engine.connect() as connection:
        index = (
            await connection.execute(text("select 1 from pg_indexes where indexname = 'uq_pipeline_runs_one_active'"))
        ).scalar()
    assert index == 1, "run `alembic upgrade head` on this database first"
    # the tests count runs, so they start from nothing. This empties every table that belongs to a tenant
    async with asyncio_engine.begin() as connection:
        await connection.execute(text("TRUNCATE tenants RESTART IDENTITY CASCADE"))
    await redis_client.delete(QUEUE_NAME)
    yield
    await redis_client.delete(QUEUE_NAME)
    await redis_client.connection_pool.disconnect()
    await asyncio_engine.dispose()


class _Source(http.server.BaseHTTPRequestHandler):
    """A data source the ingestion step can read: /ok.csv at once, /slow.csv?secs=N after a wait, /fail with a 500."""

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/fail":
            self.send_response(500)
            self.end_headers()
            return
        if parsed.path == "/slow.csv":
            time.sleep(float(urllib.parse.parse_qs(parsed.query).get("secs", ["1"])[0]))
        body = b"id,value\n1,10\n2,20\n3,30\n"
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


@pytest.fixture(scope="session")
def source():
    """The base URL of a local data source, for the whole test session."""
    class Server(socketserver.ThreadingMixIn, http.server.HTTPServer):
        daemon_threads = True
        request_queue_size = 128  # the default of 5 refuses the connections of six workers fetching at once

    server = Server(("127.0.0.1", 0), _Source)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{server.server_address[1]}"
    server.shutdown()


@pytest.fixture(autouse=True)
def fetch_without_a_network(request, monkeypatch):
    """The ingestion step reads a URL. These tests are about the database and the queue, not HTTP, so the fetch is
    replaced by one that answers at once (or after the delay a /slow.csv?secs=N URL asks for) without a socket.
    A test that needs a real separate process is marked real_http and uses the real data source."""
    if request.node.get_closest_marker("real_http"):
        return
    import asyncio

    async def fetch_data(url):
        parsed = urllib.parse.urlparse(url)
        if parsed.path == "/fail":
            raise ConnectionError("the source answered 500")
        if parsed.path == "/slow.csv":
            await asyncio.sleep(float(urllib.parse.parse_qs(parsed.query).get("secs", ["1"])[0]))
        return CSV

    monkeypatch.setattr("worker.app.step_handlers.fetch_data", fetch_data)


@pytest.fixture
async def world(source):
    """A tenant with a pipeline that has one ingestion step, and a function that makes more pipelines."""
    return await helpers.World.create(source)
