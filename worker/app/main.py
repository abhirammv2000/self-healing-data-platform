from worker.app.executor import run_executor_service
from worker.app.exceptions import InvalidPipelineRunStatusError
from worker.app.reaper import reaper_loop
from worker.app.scheduler import scheduler_loop
from shared.config import WORKER_METRICS_PORT, validate_settings
from shared.redis_client import redis_client
from shared.run_queue import QUEUE_NAME
from shared.observability import setup_tracing, setup_logging, get_logger
from prometheus_client import start_http_server
import asyncio
import importlib
import signal
import sys

#an end-to-end run on Windows hit UnicodeEncodeError from a plain print() containing a
#"->" arrow character (U+2192), because the default console codepage (cp1252) doesn't have it.
#That exception wasn't a SQLAlchemyError, so it wasn't caught by the persistence code's own
#handler; it propagated out and made a successful diagnostic-agent write look like a failure
#to the executor's caller. The specific string that triggered it is fixed at the source too
#(see diagnostic_agent.py), but reconfiguring stdout here means any other non-ASCII text this
#process ever prints, including exception messages from libraries this code doesn't control,
#can't crash the worker loop the same way. errors="replace" swaps an unencodable character
#for "?" in the log rather than raising, which is the right tradeoff for a log stream.
if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

setup_tracing("worker")
setup_logging()
log=get_logger(__name__)

#the worker is a plain asyncio loop, not a FastAPI app, so it has no /metrics endpoint the way
#the control plane gets one from prometheus-fastapi-instrumentator. prometheus_client metrics
#are per-process: pipeline_runs_total and the diagnostic-agent counters showed up empty when
#only the control plane was scraped, because they're incremented in this process. So the worker
#needs its own scrape target. start_http_server is the standard prometheus_client pattern for a
#long-running background process, see observability/prometheus.yml's second job. It is started in main(),
#not on import, so importing this module (the tests do) does not open a port.

BACKGROUND_TASKS=[]

def install_shutdown_handlers(loop, shutdown):
    """Set `shutdown` when the process is asked to stop (SIGTERM from Kubernetes or docker, Ctrl+C).

    Only the first signal is a request. A second one is left to the default handler, so a person who presses Ctrl+C
    twice still gets out at once.
    """
    names=["SIGTERM","SIGINT","SIGBREAK"] #SIGBREAK exists on Windows only
    for name in names:
        sig=getattr(signal,name,None)
        if sig is None:
            continue
        try:
            def request_stop(sig=sig):
                log.info("worker_shutdown_requested", signal=signal.Signals(sig).name)
                shutdown.set()
                loop.remove_signal_handler(sig)
            loop.add_signal_handler(sig, request_stop)
        except NotImplementedError:
            #Windows event loops cannot do add_signal_handler
            def request_stop_threadsafe(received, frame, sig=sig):
                loop.call_soon_threadsafe(shutdown.set)
                signal.signal(sig, signal.SIG_DFL)
            signal.signal(sig, request_stop_threadsafe)

def warm_up_agent():
    """Import the diagnostic agent now, before any run is taken.

    Its libraries are heavy: the first import took 50 seconds on a slow machine. Done lazily, that happens in the middle
    of the first failed run, blocks the event loop for all that time, and the lease heartbeat cannot run meanwhile. A
    broken agent dependency must still not stop the worker from starting, so a failure is logged and the executor
    meets it again, and handles it, when a run fails.
    """
    try:
        importlib.import_module("worker.app.agent.diagnostic_agent")
    except Exception as e:
        log.error("agent_import_failed", error=str(e))

async def worker_loop(shutdown):
    """Take runs off the queue one at a time until `shutdown` is set.

    The run in hand is finished before the loop looks at `shutdown` again, so a worker that is told to stop does not
    abandon a run half way. The wait on the queue is short so a stop request is noticed within a second.
    """
    while not shutdown.is_set():
        try:
            result=await redis_client.blpop(QUEUE_NAME, timeout=1)

            if result is None:
                continue

            queue_name,value=result
            run_id=int(value.decode("utf-8"))
            log.info("run_picked_up", run_id=run_id, queue=queue_name.decode("utf-8"))

            try:
                await run_executor_service(run_id)
            except InvalidPipelineRunStatusError as e:
                #the id was on the queue more than once, or the run was claimed by someone else first. Not a failure.
                log.info("run_already_handled", run_id=run_id, detail=str(e))
            except Exception as e: #generic exception for now
                log.error("run_execution_failed", run_id=run_id, error=str(e))

        except Exception as e: #generic exception for now
            log.error("worker_loop_error", error=str(e))
            await asyncio.sleep(1)

async def main():
    #a missing or weak secret stops the worker here, named, instead of failing on the first run that needs it
    validate_settings()
    warm_up_agent()
    start_http_server(WORKER_METRICS_PORT)
    shutdown=asyncio.Event()
    install_shutdown_handlers(asyncio.get_running_loop(), shutdown)
    log.info("worker_started", queue=QUEUE_NAME, metrics_port=WORKER_METRICS_PORT)

    #the reaper gets back runs that the queue or another worker lost, see worker/app/reaper.py. Every worker runs
    #one, which is safe because the rows it takes are locked and a run can only be claimed once.
    #the task is kept in a list because asyncio only holds a weak reference to a running task, and it could be
    #collected while the loop below runs
    BACKGROUND_TASKS.append(asyncio.create_task(reaper_loop(shutdown)))
    #the scheduler starts a run when a pipeline's cron time comes, see worker/app/scheduler.py. Every worker runs
    #one, which is safe because the run for one cron time is created once, whoever tries
    BACKGROUND_TASKS.append(asyncio.create_task(scheduler_loop(shutdown)))

    await worker_loop(shutdown)

    #the run in hand is done. The reaper and the scheduler see the same stop request and end after the pass they are
    #in. If one is stuck, it is cancelled after a few seconds: nothing it holds can be lost, because it commits row
    #by row and another worker's reaper and scheduler carry on
    done,pending=await asyncio.wait(BACKGROUND_TASKS, timeout=10)
    for task in pending:
        task.cancel()
    await asyncio.gather(*pending, return_exceptions=True)
    log.info("worker_stopped")

if __name__=="__main__":
    asyncio.run(main())
