"""Tracing (OpenTelemetry) and structured logging (structlog), set up so every log line
written during a trace carries that trace's trace_id and span_id.

Everything is local and free. Spans go over OTLP gRPC (port 4317) to the Jaeger container
in docker-compose.yml, with no vendor and no API key. I tested it against
opentelemetry-sdk==1.44.0 and structlog==26.1.0 before writing it, since the API changes
between versions.

Two entry points, each called once at startup in control_plane/app/main.py and
worker/app/main.py:
  setup_tracing(service_name)
  setup_logging()

The rest of the code uses get_logger(name) instead of print().
"""
from __future__ import annotations

import logging

import structlog
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from shared.config import OTEL_EXPORTER_OTLP_ENDPOINT, OTEL_TRACES_ENABLED

_tracing_configured = False
_logging_configured = False


def setup_tracing(service_name: str) -> None:
    """Idempotent on purpose: both entry points can get called more than
    once in a process (e.g. a test importing the app module twice), and the
    OTel SDK doesn't support re-registering a TracerProvider."""
    global _tracing_configured
    if _tracing_configured or not OTEL_TRACES_ENABLED:
        return

    resource = Resource.create({"service.name": service_name})
    provider = TracerProvider(resource=resource)
    exporter = OTLPSpanExporter(endpoint=OTEL_EXPORTER_OTLP_ENDPOINT, insecure=True)
    #BatchSpanProcessor exports asynchronously on a background thread and retries
    #transient failures. With no Jaeger container running, it logs a retry
    #warning and keeps going rather than raising into application code, so a
    #trace exporter outage can't fail a pipeline run.
    provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)
    _tracing_configured = True


def get_tracer(name: str):
    return trace.get_tracer(name)


def _add_trace_context(logger, method_name, event_dict):
    """structlog processor: stamps the active span's trace_id/span_id onto
    every log event, or leaves the event alone when there's no active span
    (most log lines outside a request/task won't have one)."""
    span = trace.get_current_span()
    ctx = span.get_span_context()
    if ctx.is_valid:
        event_dict["trace_id"] = format(ctx.trace_id, "032x")
        event_dict["span_id"] = format(ctx.span_id, "016x")
    return event_dict


def setup_logging() -> None:
    global _logging_configured
    if _logging_configured:
        return

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            _add_trace_context,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )
    _logging_configured = True


def get_logger(name: str):
    return structlog.get_logger(name)
