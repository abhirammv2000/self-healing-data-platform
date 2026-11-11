import os
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL=os.getenv("DATABASE_URL")
DATABASE_URL_SYNC=os.getenv("DATABASE_URL_SYNC")
REDIS_URL=os.getenv("REDIS_URL")
ENV=os.getenv("ENV", "dev")
DATA_WAREHOUSE_URL=os.getenv("DATA_WAREHOUSE_URL") #local data warehouse url (2nd DB on postgres)
API_KEY_SECRET=os.getenv("API_KEY_SECRET")
ADMIN_SECRET_KEY=os.getenv("ADMIN_SECRET_KEY")
GOOGLE_API_KEY=os.getenv("GOOGLE_API_KEY")
GEMINI_MODEL=os.getenv("GEMINI_MODEL", "gemini-3.6-flash") #defaults to flash if unset, so things keep working even when env is misconfigured.
#was gemini-2.5-flash until 2026-09-19, when it started 404ing ("no longer available to new users") despite still listing in the models-list endpoint.
#Caught only because the eval harness (see eval/) started returning sentinel-only output; graceful degradation swallowed the error silently.
#gemini-3.6-flash is the replacement Google's 404 response named, and it works with with_structured_output().
GEMINI_EMBEDDING_MODEL=os.getenv("GEMINI_EMBEDDING_MODEL", "gemini-embedding-001") #defaults to gemini-embedding-001

#observability, see shared/observability.py. Points at the local Jaeger container's OTLP
#gRPC receiver (docker-compose.yml). No hosted vendor, no API key, no cost.
OTEL_EXPORTER_OTLP_ENDPOINT=os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317")
OTEL_TRACES_ENABLED=os.getenv("OTEL_TRACES_ENABLED", "true").strip().lower() not in ("false", "0", "")

def _positive_int(name, default):
    """A whole number above zero from the environment. A wrong value stops the process at startup and names the variable."""
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        value = int(raw)
    except ValueError:
        raise ValueError(f"{name} must be a whole number, got {raw!r}") from None
    if value < 1:
        raise ValueError(f"{name} must be at least 1, got {raw!r}")
    return value


# how long the worker that claimed a run owns it without saying so again. The worker extends it every third of this
RUN_LEASE_SECONDS = _positive_int("RUN_LEASE_SECONDS", 60)
# how often a worker looks for runs the queue or another worker lost
REAPER_INTERVAL_SECONDS = _positive_int("REAPER_INTERVAL_SECONDS", 10)
# a queued run that was put on the queue longer ago than this, and has not been claimed, is put there again
ENQUEUE_VISIBILITY_SECONDS = _positive_int("ENQUEUE_VISIBILITY_SECONDS", 30)
# a run whose worker keeps stopping is failed after this many claims
RUN_MAX_ATTEMPTS = _positive_int("RUN_MAX_ATTEMPTS", 3)
# the port a worker serves /metrics on. Two workers on one machine need different ports
WORKER_METRICS_PORT = _positive_int("WORKER_METRICS_PORT", 8001)
