from worker.app.executor import run_executor_service
from shared.redis_client import redis_client
from shared.observability import setup_tracing, setup_logging, get_logger
from prometheus_client import start_http_server
import asyncio
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
#long-running background process, see observability/prometheus.yml's second job.
start_http_server(8001)

QUEUE_NAME="pipeline_runs"

async def main():
    log.info("worker_started", queue=QUEUE_NAME)

    while True:
        try:
            result=await redis_client.blpop(QUEUE_NAME)

            if result is None:
                continue

            queue_name,value=result
            run_id=int(value.decode("utf-8"))
            log.info("run_picked_up", run_id=run_id, queue=queue_name.decode("utf-8"))

            try:
                await run_executor_service(run_id)
            except Exception as e: #generic exception for now
                log.error("run_execution_failed", run_id=run_id, error=str(e))

        except Exception as e: #generic exception for now
            log.error("worker_loop_error", error=str(e))
            await asyncio.sleep(1)

if __name__=="__main__":
    asyncio.run(main())