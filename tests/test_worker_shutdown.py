"""A worker told to stop finishes the run it has, takes no new one, and exits.

The queue and the executor are replaced. These tests check the loop's decisions, not Redis. The real process, with a
real signal, is in tests/integration/test_worker_process.py.
"""
import asyncio
import signal
from unittest.mock import MagicMock

import pytest

from worker.app import main as worker_main


class FakeQueue:
    """Hands out run ids one by one, then answers like an empty queue after the timeout."""

    def __init__(self, ids):
        self.ids = list(ids)
        self.calls = []

    async def blpop(self, name, timeout=0):
        self.calls.append(timeout)
        if self.ids:
            return (b"shdp:runs", str(self.ids.pop(0)).encode())
        await asyncio.sleep(0)
        return None


@pytest.fixture
def queue(monkeypatch):
    fake = FakeQueue([1, 2, 3])
    monkeypatch.setattr(worker_main, "redis_client", fake)
    return fake


async def test_the_run_in_hand_is_finished_and_no_new_one_is_taken(queue, monkeypatch):
    shutdown = asyncio.Event()
    finished = []

    async def run(run_id):
        if run_id == 1:
            shutdown.set()  # the stop request arrives while run 1 is being worked on
            await asyncio.sleep(0.05)
        finished.append(run_id)

    monkeypatch.setattr(worker_main, "run_executor_service", run)

    await asyncio.wait_for(worker_main.worker_loop(shutdown), timeout=2)

    assert finished == [1]  # run 1 ran to its end, runs 2 and 3 were left on the queue
    assert queue.ids == [2, 3]


async def test_a_worker_told_to_stop_before_it_starts_does_not_ask_the_queue(monkeypatch):
    queue = FakeQueue([])
    monkeypatch.setattr(worker_main, "redis_client", queue)
    shutdown = asyncio.Event()
    shutdown.set()

    await asyncio.wait_for(worker_main.worker_loop(shutdown), timeout=2)

    assert queue.calls == []


async def test_the_wait_on_the_queue_is_bounded_so_a_stop_request_is_noticed(queue, monkeypatch):
    shutdown = asyncio.Event()

    async def run(run_id):
        shutdown.set()

    monkeypatch.setattr(worker_main, "run_executor_service", run)
    await worker_main.worker_loop(shutdown)

    assert queue.calls and all(timeout and timeout > 0 for timeout in queue.calls)


async def test_a_failing_run_does_not_stop_the_loop(queue, monkeypatch):
    shutdown = asyncio.Event()
    seen = []

    async def run(run_id):
        seen.append(run_id)
        if run_id == 1:
            raise RuntimeError("boom")
        if run_id == 2:
            shutdown.set()

    monkeypatch.setattr(worker_main, "run_executor_service", run)
    await asyncio.wait_for(worker_main.worker_loop(shutdown), timeout=2)

    assert seen == [1, 2]


def test_the_first_signal_asks_for_a_stop_and_a_second_is_left_to_the_default():
    loop = MagicMock()
    shutdown = MagicMock()
    registered = {}
    loop.add_signal_handler.side_effect = lambda sig, callback: registered.__setitem__(sig, callback)

    worker_main.install_shutdown_handlers(loop, shutdown)

    assert signal.SIGTERM in registered and signal.SIGINT in registered
    registered[signal.SIGTERM]()
    shutdown.set.assert_called_once()
    loop.remove_signal_handler.assert_called_once_with(signal.SIGTERM)


def test_where_the_loop_cannot_install_handlers_the_signal_module_is_used(monkeypatch):
    loop = MagicMock()
    loop.add_signal_handler.side_effect = NotImplementedError
    shutdown = MagicMock()
    installed = {}
    monkeypatch.setattr(worker_main.signal, "signal", lambda sig, handler: installed.__setitem__(sig, handler))

    worker_main.install_shutdown_handlers(loop, shutdown)

    assert signal.SIGTERM in installed
    installed[signal.SIGTERM](signal.SIGTERM, None)
    loop.call_soon_threadsafe.assert_called_once_with(shutdown.set)
    assert installed[signal.SIGTERM] == signal.SIG_DFL  # the next one is not caught


async def test_main_refuses_to_start_with_a_bad_configuration(monkeypatch):
    monkeypatch.setattr(worker_main, "validate_settings", MagicMock(side_effect=RuntimeError("invalid configuration: x")))
    start = MagicMock()
    monkeypatch.setattr(worker_main, "start_http_server", start)
    monkeypatch.setattr(worker_main, "warm_up_agent", lambda: None)

    with pytest.raises(RuntimeError, match="invalid configuration"):
        await worker_main.main()

    start.assert_not_called()


def test_a_broken_agent_import_is_logged_and_does_not_stop_the_worker(monkeypatch):
    def broken(name):
        raise ImportError("langchain is broken")

    monkeypatch.setattr(worker_main.importlib, "import_module", broken)
    worker_main.warm_up_agent()  # does not raise


def test_the_agent_is_imported_before_the_first_run(monkeypatch):
    seen = []
    monkeypatch.setattr(worker_main.importlib, "import_module", lambda name: seen.append(name))
    worker_main.warm_up_agent()
    assert seen == ["worker.app.agent.diagnostic_agent"]


async def test_main_stops_the_reaper_and_the_scheduler_after_the_loop(monkeypatch):
    monkeypatch.setattr(worker_main, "validate_settings", lambda: None)
    monkeypatch.setattr(worker_main, "warm_up_agent", lambda: None)
    monkeypatch.setattr(worker_main, "start_http_server", lambda port: None)
    monkeypatch.setattr(worker_main, "install_shutdown_handlers", lambda loop, event: None)
    stops = {}

    async def reaper(stop):
        await stop.wait()
        stops["reaper"] = True

    async def scheduler(stop):
        await stop.wait()
        stops["scheduler"] = True

    async def loop_that_ends(shutdown):
        shutdown.set()

    monkeypatch.setattr(worker_main, "reaper_loop", reaper)
    monkeypatch.setattr(worker_main, "scheduler_loop", scheduler)
    monkeypatch.setattr(worker_main, "worker_loop", loop_that_ends)
    worker_main.BACKGROUND_TASKS.clear()

    await asyncio.wait_for(worker_main.main(), timeout=5)

    assert stops == {"reaper": True, "scheduler": True}
