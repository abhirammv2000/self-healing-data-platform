"""Custom domain metrics, registered on the default Prometheus registry so
prometheus-fastapi-instrumentator's /metrics endpoint (wired up in
control_plane/app/main.py) exposes these alongside its automatic HTTP
request/latency metrics, all from one scrape target.

These live in shared/, not control_plane/, because the worker process (a
separate Python process, no FastAPI app of its own) also increments them,
and prometheus_client metrics are per-process: the worker's counts never
reached the control plane's /metrics endpoint until it got its own scrape
target. worker/app/main.py calls prometheus_client's start_http_server() to
expose /metrics on port 8001, scraped as a second job in
observability/prometheus.yml. A pushgateway is for short-lived batch jobs;
this worker runs continuously, so it exposes /metrics directly instead.
"""
from prometheus_client import Counter, Histogram

pipeline_runs_total = Counter(
    "pipeline_runs_total",
    "Pipeline runs, counted once each when they reach a terminal status.",
    ["status"],  # "success" or "failed", matching PipelineRun.status's terminal values
)

pipeline_run_duration_seconds = Histogram(
    "pipeline_run_duration_seconds",
    "Wall-clock time from a run being claimed (queued -> running) to reaching a terminal status.",
)

diagnostic_agent_classifications_total = Counter(
    "diagnostic_agent_classifications_total",
    "Diagnostic agent failure classifications, including the sentinel value on a degraded run.",
    ["classification"],  # network, quota, schema, partial_load, unknown
)

diagnostic_agent_recommendations_total = Counter(
    "diagnostic_agent_recommendations_total",
    "Diagnostic agent recommended actions, including the sentinel value on a degraded run.",
    ["action"],  # retry, retry_with_backoff, schema_evolution, replay_from_raw, escalate, pause_schedule
)

circuit_breaker_transitions_total = Counter(
    "circuit_breaker_transitions_total",
    "Circuit breaker state transitions, counted at the moment a pipeline's breaker changes state.",
    ["to_state"],  # "open" or "closed", the only two transitions executor.py triggers
)
