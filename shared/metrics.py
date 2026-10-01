"""Custom domain metrics, registered on the default Prometheus registry.
prometheus-fastapi-instrumentator exposes them at /metrics (set up in
control_plane/app/main.py) next to its HTTP metrics, so one scrape target covers both.

They live in shared/ and not control_plane/ because the worker also increments them. The
worker is a separate process and prometheus_client metrics are per process, so its counts
never reached the control plane's /metrics. worker/app/main.py now calls
start_http_server() to serve /metrics on port 8001, and observability/prometheus.yml
scrapes it as a second job. A pushgateway is meant for short-lived jobs and this worker
runs all the time, so it serves /metrics itself.
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
