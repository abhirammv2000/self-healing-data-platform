# Self-Healing Data Pipeline Platform

> **Status: work in progress.** The control plane, worker/executor, and the LangGraph diagnostic agent with pgvector RAG are all built and working end-to-end. Containerization, IaC, and the agent evaluation harness are still on the [roadmap](#roadmap).

A multi-tenant, event-driven data pipeline orchestration platform with an LLM-powered diagnostic layer. Pipelines are defined and managed through a REST control plane, executed asynchronously by a queue-driven worker with built-in resilience (retries, circuit breaking), and — when a run fails — automatically diagnosed by a LangGraph multi-agent system that classifies the failure, retrieves relevant operational context via RAG, and recommends a recovery action for operator review.

> **On "self-healing":** the platform automates real resilience behaviors — per-step retries with exponential backoff, and per-pipeline circuit breakers that stop cascading failures. The agent layer adds **diagnosis and recommendation**: every recommendation is written with `status="pending"` and a human decides whether to apply or dismiss it. The agent does **not** auto-execute recovery actions. That boundary is deliberate.

---

## Table of contents

- [Architecture overview](#architecture-overview)
- [Key capabilities](#key-capabilities)
- [Tech stack](#tech-stack)
- [Data model](#data-model)
- [Run lifecycle](#run-lifecycle)
- [The diagnostic agent](#the-diagnostic-agent)
- [Retrieval-augmented generation (RAG)](#retrieval-augmented-generation-rag)
- [Project structure](#project-structure)
- [Getting started](#getting-started)
- [Configuration](#configuration)
- [Database migrations](#database-migrations)
- [Running the services](#running-the-services)
- [Indexing runbooks](#indexing-runbooks)
- [Testing](#testing)
- [Design principles](#design-principles)
- [Roadmap](#roadmap)

---

## Architecture overview

The system is split into three top-level components that share a common library:

| Component | Responsibility |
|-----------|----------------|
| **Control plane** (`control_plane/`) | FastAPI REST API. Owns all resource *creation* and lifecycle management — tenants, pipelines, steps, schedules, runs, circuit breakers, recommendations. Handles authentication and tenant isolation. |
| **Worker** (`worker/`) | Redis-queue-driven executor. Owns run *execution* — claims queued runs, executes pipeline steps with retries, evaluates circuit breakers, records observability, dispatches webhooks, and invokes the diagnostic agent on failure. |
| **Shared** (`shared/`) | Cross-cutting infrastructure used by both: database engines/sessions, Redis client, config, embeddings helpers, the webhook dispatcher, and utilities. |

The two services communicate through **Postgres** (shared source of truth) and **Redis** (a job queue). The control plane enqueues a `run_id`; the worker picks it up and executes it. There is no direct service-to-service HTTP coupling.

A clean separation underpins the whole design: **the control plane owns creation, the worker owns execution.** This keeps the run lifecycle unambiguous and makes each side independently reasonable about.

---

## Key capabilities

- **Multi-tenancy** — every resource is tenant-scoped. All tenant-facing queries filter on `tenant_id + resource_id` to enforce ownership, preventing cross-tenant access.
- **Two-tier authentication** — an admin secret (`X-Admin-Secret`, constant-time compared) gates tenant provisioning; per-tenant API keys (`X-Api-Key`, hashed at rest) gate all tenant-scoped operations. API keys are auto-created on tenant creation via a flush-before-commit pattern.
- **Event-driven execution** — the worker blocks on a Redis list (`BLPOP`) and processes runs as they arrive, decoupling enqueue from execute.
- **Configurable step pipeline** — pipelines are ordered sequences of steps. Built-in step handlers: `ingestion` (HTTP fetch via `httpx`), `validation` (schema/null/row-count checks via `pandas`), `transformation` (rename / filter / drop), and `load` (writes to a separate data-warehouse Postgres DB). Steps share a `run_context` dict so later steps can read earlier outputs.
- **Automatic retries** — per-step retry config supports `exponential_backoff` and `exponential_backoff_jitter` strategies.
- **Circuit breakers** — per-pipeline breakers (configurable failure threshold + rolling window) open to block runs after repeated failures, with `closed → open → half-open` state transitions. State lives in Postgres (source of truth).
- **Always-on observability** — every run writes a `run_context.json` capturing step attempts, timing, and outcomes, recorded in a `finally` block so it's captured regardless of how the run ends.
- **Webhook callbacks** — fire-and-forget delivery (via `asyncio.create_task`) on run completion/failure, with full delivery audit records. Failed-run payloads include a `recommendations_url` pointing at the agent's diagnosis.
- **Multi-agent failure diagnosis** — a LangGraph graph of specialized nodes (log analysis → classification → retrieval → recovery planning) produces a structured, grounded recommendation for every failed run.
- **RAG-grounded recommendations** — the agent retrieves relevant runbook sections and the tenant's own past incidents from a pgvector store, so recommendations cite operational knowledge and history rather than reasoning from the error string alone.

---

## Tech stack

**Core:** Python · FastAPI · SQLAlchemy (async) · Pydantic · PostgreSQL · Redis · Alembic

**Agent / ML:** LangGraph · LangChain · `langchain-google-genai` · Google Gemini (chat + embeddings) · pgvector

**Data processing:** pandas · httpx

**Infra / tooling:** Docker Compose (Postgres + Redis for local dev) · uvicorn · croniter

Gemini was chosen over OpenAI because the deployment target is **GCP Cloud Run**, keeping the model provider aligned with the cloud platform. `langchain-google-genai` (rather than the direct Gemini client) is used for first-class LangGraph compatibility.

---

## Data model

Ten tables in the control-plane Postgres database (the data-warehouse DB is separate and holds only pipeline `load` output).

| Table | Purpose |
|-------|---------|
| `tenants` | Top-level tenant accounts; `is_active` gates all access. |
| `api_keys` | Hashed per-tenant API keys; auto-created with each tenant; revocable. |
| `pipelines` | Pipeline definitions, including an optional `callback_url` for webhooks. |
| `pipeline_steps` | Ordered steps belonging to a pipeline, each with a `step_type` and JSON `config`. |
| `schedules` | Cron-style schedules for pipelines. |
| `pipeline_runs` | Individual execution records; status, timing, `error_type`, `error_message`. |
| `pipeline_circuit_breakers` | Per-pipeline breaker state, failure threshold, and rolling window config. |
| `webhook_callbacks` | Audit trail of webhook delivery attempts and outcomes. |
| `agent_recommendations` | Diagnostic-agent output: classification, recommended action, explanation, status. |
| `incident_embeddings` | pgvector store for RAG — runbook chunks, past run failures, and past recommendations. |

A couple of intentional schema decisions worth calling out:

- **`error_type` stores the exception class name** (`type(e).__name__`), not the step type. This gives the agent a much better classification signal than knowing merely which step failed.
- **`incident_embeddings` uses a polymorphic `source_type` + `source_id`** (`'runbook' | 'past_run' | 'past_recommendation'`) rather than separate tables or hard FKs, so the source set can grow without migrations. Tenant-scoped rows cascade on tenant deletion; runbook rows are global (`tenant_id IS NULL`).

---

## Run lifecycle

1. **Enqueue** — the control plane creates a `pipeline_run` (`status="queued"`) and pushes its `run_id` onto the Redis `pipeline_runs` queue.
2. **Pick up** — the worker's `BLPOP` loop receives the `run_id`.
3. **Circuit-breaker gate** — if the pipeline's breaker is `open`, the run is marked `blocked` and execution stops.
4. **Atomic claim** — the run is flipped `queued → running` in a single conditional `UPDATE`, so two workers can never both claim the same run.
5. **Execute steps** — steps run in `step_order`. Each step retries per its config (exponential backoff, optionally with jitter). Every attempt is recorded in `run_context["step_attempts"]`.
6. **Resolve outcome** —
   - **Success:** run marked `success`.
   - **Failure:** run marked `failed` with `error_type` / `error_message`; the circuit breaker re-evaluates (a `half-open` breaker re-opens immediately; a `closed` breaker opens if failures in the window exceed the threshold).
7. **`finally` block (always runs):**
   1. **Observability** — write `run_context.json`.
   2. **Diagnostic agent** *(failed runs only)* — `await`ed (not fire-and-forget) so the resulting `recommendation_id` is available for the webhook payload.
   3. **Webhook** — fire-and-forget dispatch including a `recommendations_url` when a recommendation was produced.

The agent is invoked via an import *inside* the `try` block so a broken agent dependency can never prevent the worker from starting, and the agent itself is fully defensive — it catches all its own errors and returns `None` on any failure, so it can never break the executor.

---

## The diagnostic agent

When a run fails, `run_diagnostic_agent(run_context)` invokes a compiled **LangGraph** graph. The graph is built once at module load (state lives in the per-invoke initial state + checkpointer, so a single compiled graph is safely reused across runs).

**Topology** — linear, explicit, no cycles:

```
log_analysis → classification → retrieval → recovery_planning → END
```

| Node | Job |
|------|-----|
| **log_analysis** | *Describes* what happened — the failed step, the attempt pattern, an interpretation of the error. Deliberately does **not** classify or recommend, so classification isn't just rubber-stamping. |
| **classification** | Assigns a single failure category (`network` / `quota` / `schema` / `partial_load` / `unknown`) plus a `0.0–1.0` confidence and reasoning. |
| **retrieval** | Deterministic (non-LLM) RAG node. Embeds a synthesized query and pulls relevant runbook + past-incident context from pgvector. See [RAG](#retrieval-augmented-generation-rag). |
| **recovery_planning** | Recommends a concrete action (`retry` / `retry_with_backoff` / `schema_evolution` / `replay_from_raw` / `escalate` / `pause_schedule`) and writes the human-facing explanation, citing retrieved sources. |

**Structured outputs** — each LLM node uses `with_structured_output()` against a focused Pydantic schema, so the model is constrained to produce JSON of the exact shape. Downstream code writes these values straight into typed DB columns, eliminating a whole class of parsing bugs and giving the future eval harness deterministic field access.

**Graceful degradation** — every node catches its own exceptions and returns a *sentinel* output instead of crashing the graph. Sentinels leave a breadcrumb (`notable_signals: ["log_analysis_failed"]`, `confidence=0.0`, `recommended_action="escalate"`) so a degraded diagnosis is honestly marked as such and safely escalated to a human. The result: a recommendation is written for **almost every** failed run, even when the agent is partially degraded.

**Checkpointing** — an in-process `MemorySaver` keyed by `thread_id=str(run_id)`. Diagnostic runs complete in seconds with no human-in-the-loop pauses, so durable checkpointing isn't needed yet; swapping in `PostgresSaver` later is a one-line change.

After a recommendation is persisted, the agent fire-and-forget indexes the incident back into the RAG store (see below), so the system's history compounds over time.

---

## Retrieval-augmented generation (RAG)

The recovery-planning recommendation is grounded in two kinds of retrieved context:

- **Runbooks** — operational knowledge (what *should* happen for a given failure type), authored as markdown in `worker/app/agent/runbooks/` (`network.md`, `quota.md`, `schema.md`, `partial_load.md`, `unknown.md`). Global, not tenant-scoped.
- **Past incidents** — the tenant's own history: past run failures and past recommendations. Tenant-scoped, so retrieval never leaks across tenants.

**Embeddings** — Gemini `gemini-embedding-001`, truncated to **768 dimensions** (Matryoshka Representation Learning makes the leading dimensions independently meaningful; truncating costs ~0.26% quality for 4× savings on storage, index size, and latency) and **L2-normalized** so pgvector's cosine distance behaves correctly. The 768 value is a single source of truth shared by the embeddings helper, the `incident_embeddings.embedding` column, and the vector index — all three must move together if the model ever changes.

**Asymmetric retrieval** — queries embed with `RETRIEVAL_QUERY` and documents with `RETRIEVAL_DOCUMENT` task types, which materially improves retrieval quality. The embeddings client relies on LangChain's per-method defaults for this rather than a constructor override.

**Split-then-combine retrieval** — rather than a single top-K over the whole store (which would skew toward whichever source type has more rows), the retrieval node pulls the top 2 runbook chunks and top 3 tenant incident chunks separately, then merges and re-sorts globally by similarity. This guarantees breadth — operational knowledge *and* tenant history — while still letting true similarity decide final ordering.

**Two indexers:**

- `index_runbooks.py` — a CLI script (`python -m worker.app.agent.index_runbooks`) that chunks runbooks by markdown header, batch-embeds them, and **fully replaces** all `runbook` rows in one transaction. Idempotent: disk is the source of truth, the DB is a derived index. Re-run it whenever a runbook changes.
- `index_incident.py` — called fire-and-forget by the agent after each recommendation is persisted. Writes two tenant-scoped rows (one `past_run`, one `past_recommendation`) with synthesized, deterministic embedding text so semantically similar failures cluster in vector space.

---

## Project structure

```
.
├── alembic/                          # Migrations (one per table + alterations)
│   ├── versions/
│   ├── env.py
│   └── script.py.mako
├── control_plane/
│   └── app/
│       ├── models/                   # SQLAlchemy ORM models (one per table)
│       ├── routes/                   # FastAPI routers (one per resource)
│       ├── schemas/                  # Pydantic request/response schemas
│       ├── services/                 # Business logic (DB access, domain rules)
│       ├── main.py                   # FastAPI app entrypoint
│       ├── dependencies.py           # Auth dependencies (admin + tenant)
│       └── exceptions.py             # Domain exception classes
├── worker/
│   └── app/
│       ├── executor.py               # Core execution loop
│       ├── step_handlers.py          # ingestion / validation / transformation / load
│       ├── main.py                   # Worker entrypoint (Redis BLPOP loop)
│       ├── exceptions.py
│       └── agent/
│           ├── diagnostic_agent.py   # LangGraph graph construction + entrypoint
│           ├── nodes.py              # log_analysis / classification / recovery_planning
│           ├── retrieval_node.py     # RAG retrieval (pgvector)
│           ├── state.py              # Graph state (TypedDict) + sentinels
│           ├── schemas.py            # Structured-output Pydantic models
│           ├── prompts.py            # Node prompt templates
│           ├── index_runbooks.py     # Runbook indexer (CLI)
│           ├── index_incident.py     # Incident indexer (fire-and-forget)
│           └── runbooks/             # Operational knowledge (markdown)
├── shared/
│   ├── config.py                     # Env-var configuration
│   ├── db.py                         # Async + sync engines, sessions, Base
│   ├── redis_client.py               # Redis client
│   ├── embeddings.py                 # Gemini embedding helpers (sync + async)
│   ├── webhook_dispatcher.py         # Webhook delivery + audit
│   └── utils.py                      # Shared utilities
├── docker-compose.yml                # Postgres + Redis for local dev
├── init-2nd-db.sql                   # Creates the separate data-warehouse DB
├── alembic.ini
├── requirements.txt
├── .env.example
└── README.md
```

---

## Getting started

### Prerequisites

- Python 3.11+
- Docker & Docker Compose (for Postgres + Redis)
- A Google AI (Gemini) API key

### Setup

```bash
# 1. Clone and create a virtual environment
python -m venv data-platform-env
# Windows (PowerShell):
.\data-platform-env\Scripts\Activate.ps1
# macOS/Linux:
# source data-platform-env/bin/activate

# 2. Install dependencies
pip install -r requirements.txt

# 3. Configure environment
cp .env.example .env        # then fill in the values (see Configuration below)
# Windows (PowerShell): Copy-Item .env.example .env

# 4. Start Postgres + Redis
docker compose up -d

# 5. Apply migrations
alembic upgrade head

# 6. Index the runbooks (seeds the RAG store)
python -m worker.app.agent.index_runbooks
```

> **Note:** run all commands from the **project root** with full module import paths (e.g. `python -m worker.app.main`). This keeps imports consistent across local dev and future containerized deployment.

---

## Configuration

All configuration is read from environment variables (loaded from `.env` via `python-dotenv`). See `.env.example` for the template.

| Variable | Description |
|----------|-------------|
| `DATABASE_URL` | Async Postgres URL for the control plane (asyncpg driver). |
| `DATABASE_URL_SYNC` | Sync Postgres URL — used by Alembic and the runbook indexer. |
| `DATA_WAREHOUSE_URL` | Sync Postgres URL for the separate data-warehouse DB (pipeline `load` output). |
| `REDIS_URL` | Redis connection URL for the job queue. |
| `ADMIN_SECRET_KEY` | Secret for admin-only endpoints (tenant provisioning). |
| `API_KEY_SECRET` | Secret used to hash tenant API keys. |
| `GOOGLE_API_KEY` | Google AI (Gemini) API key. |
| `GEMINI_MODEL` | Chat model for the agent (default `gemini-2.5-flash`). |
| `GEMINI_EMBEDDING_MODEL` | Embedding model for RAG (default `gemini-embedding-001`). |
| `ENV` | Environment label (default `dev`). |

---

## Database migrations

Migrations are managed with **Alembic**. The async app uses the asyncpg driver; Alembic runs against the sync driver (`DATABASE_URL_SYNC`).

```bash
# Apply all migrations
alembic upgrade head

# Create a new migration after changing a model
alembic revision --autogenerate -m "describe your change"

# Roll back one migration
alembic downgrade -1
```

The `incident_embeddings` migration enables the `pgvector` extension and creates the `Vector(768)` column.

---

## Running the services

Two long-running processes. Run each in its own terminal:

```bash
# Control plane (REST API) — interactive docs at http://localhost:8000/docs
uvicorn control_plane.app.main:app --reload

# Worker (queue consumer + executor + agent)
python -m worker.app.main
```

---

## Indexing runbooks

The RAG store must be seeded before the agent can ground recommendations in operational knowledge. Re-run the indexer any time a runbook is added, edited, or removed — it fully replaces all runbook rows idempotently:

```bash
python -m worker.app.agent.index_runbooks
```

Past-incident rows are indexed automatically by the agent after each failed run; no manual step is needed for those.

---

## Testing

Current verification approach (manual, evolving toward automation):

- **Endpoints** — Swagger UI at `/docs`.
- **Webhooks** — [webhook.site](https://webhook.site) to inspect delivered payloads (set a pipeline's `callback_url` to a webhook.site URL).
- **Data** — direct Postgres queries to verify run records, recommendations, circuit-breaker state, and indexed embeddings.

An automated agent **evaluation harness** (with labeled cases for classification and recommendation accuracy) is planned — see the roadmap.

---

## Design principles

A few principles applied consistently across the codebase:

- **Control plane owns creation; worker owns execution.** Clean separation of the run lifecycle.
- **Observability in `finally`, never as a step.** Run telemetry is always captured, regardless of the failure path.
- **Layered architecture** per resource: model → schema → service → routes → migration, verified via Swagger before moving on.
- **Dual filtering (`tenant_id + resource_id`)** on all tenant-scoped queries for ownership verification.
- **Narrow `try` blocks in the service layer** so `HTTPException` is never swallowed by `SQLAlchemyError` handlers.
- **`None` returns from services for 404 cases;** the route layer translates them to HTTP responses.
- **Custom exception classes** for all domain errors (`InvalidPipelineRunStatus`, `ValidationStepError`, etc.).
- **The agent is best-effort and fully defensive** — it can never break the executor, and degraded diagnoses are honestly marked and escalated.
- **Honesty over impressive language.** The system *diagnoses and recommends*; it does not autonomously remediate. The docs say exactly what's built.

---

## Roadmap

Built so far: the full control plane, the worker/executor with retries + circuit breaking + observability + webhooks, and the complete LangGraph multi-agent diagnostic system with pgvector RAG.

Planned next:

- [ ] **Agent evaluation harness** — labeled `agent_eval_cases`, accuracy metrics for classification and recommendation.
- [ ] **Recommendation status sync into RAG** — update `incident_embeddings` metadata when a recommendation is applied/dismissed (currently a known TODO; retrieval falls back to similarity-only ranking until then).
- [ ] **`Literal` type constraints** retrofitted across all API schemas (batch refactor).
- [ ] **Containerization** — Dockerfiles for the control plane and worker, targeting GCP Cloud Run.
- [ ] **Terraform** — provision GCP infrastructure (Cloud Run, Cloud SQL, Memorystore).
- [ ] **Durable checkpointing** — swap the agent's `MemorySaver` for `PostgresSaver` if/when human-in-the-loop pauses are introduced.
- [ ] **Conditional graph routing** — earned branching (e.g. low-confidence → skip straight to escalate) once there's a concrete reason for it.
