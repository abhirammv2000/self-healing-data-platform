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
# how often a worker checks whether a schedule is due. A run starts at most this long after its cron time
SCHEDULER_INTERVAL_SECONDS = _positive_int("SCHEDULER_INTERVAL_SECONDS", 10)

# the longest the diagnostic agent may take after a failed run. The model is a remote service that can be slow or down,
# and the worker and the webhook wait for the agent, so it is cut off after this and the run goes on without a
# recommendation
DIAGNOSTIC_TIMEOUT_SECONDS = _positive_int("DIAGNOSTIC_TIMEOUT_SECONDS", 120)

# the longest the diagnostic agent may take after a failed run. The model is a remote service that can be slow or down,
# and the worker and the webhook wait for the agent, so it is cut off after this and the run goes on without a
# recommendation
DIAGNOSTIC_TIMEOUT_SECONDS = _positive_int("DIAGNOSTIC_TIMEOUT_SECONDS", 120)

# the largest file an ingestion step downloads, in megabytes
MAX_DOWNLOAD_MB = _positive_int("MAX_DOWNLOAD_MB", 100)

# By default a pipeline source and a webhook may not point at a private, loopback or link-local address, because the
# worker runs inside the cluster and could be made to call its database, its Redis or the cloud metadata service.
# Set this to true only on a developer machine or where every tenant is trusted. See docs/PRODUCTION.md
ALLOW_PRIVATE_URLS = os.getenv("ALLOW_PRIVATE_URLS", "false").strip().lower() in ("true", "1", "yes")

_REQUIRED_SECRETS = ("DATABASE_URL", "DATABASE_URL_SYNC", "REDIS_URL", "DATA_WAREHOUSE_URL", "API_KEY_SECRET", "ADMIN_SECRET_KEY")
_MIN_SECRET_LENGTH = 16


def settings_problems(env=None):
    """What is wrong with the configuration, as a list of sentences. Empty when it is usable.

    Every setting is required. When ENV is "production" the two secrets must also be long and different from each
    other.
    """
    env = os.environ if env is None else env
    problems = []
    for name in _REQUIRED_SECRETS:
        if not (env.get(name) or "").strip():
            problems.append(f"{name} is not set")
    if env.get("ENV", "dev") == "production":
        api_key_secret = env.get("API_KEY_SECRET") or ""
        admin_secret = env.get("ADMIN_SECRET_KEY") or ""
        for name, value in (("API_KEY_SECRET", api_key_secret), ("ADMIN_SECRET_KEY", admin_secret)):
            if value.strip() and len(value) < _MIN_SECRET_LENGTH:
                problems.append(f"{name} must be at least {_MIN_SECRET_LENGTH} characters when ENV=production")
        if api_key_secret and api_key_secret == admin_secret:
            problems.append("API_KEY_SECRET and ADMIN_SECRET_KEY must differ when ENV=production")
    return problems


def validate_settings():
    """Stop the process at startup, naming every problem, instead of failing on the first request that needs a setting."""
    problems = settings_problems()
    if problems:
        raise RuntimeError("invalid configuration: " + "; ".join(problems))
