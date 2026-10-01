# Setup and running

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


## Configuration

All configuration is read from environment variables (loaded from `.env` via `python-dotenv`). See `.env.example` for the template.

| Variable | Description |
|----------|-------------|
| `DATABASE_URL` | Async Postgres URL for the control plane (asyncpg driver). |
| `DATABASE_URL_SYNC` | Sync Postgres URL, used by Alembic and the runbook indexer. |
| `DATA_WAREHOUSE_URL` | Sync Postgres URL for the separate data-warehouse DB (pipeline `load` output). |
| `REDIS_URL` | Redis connection URL for the job queue. |
| `ADMIN_SECRET_KEY` | Secret for admin-only endpoints (tenant provisioning). |
| `API_KEY_SECRET` | Secret used to hash tenant API keys. |
| `GOOGLE_API_KEY` | Google AI (Gemini) API key. |
| `GEMINI_MODEL` | Chat model for the agent (default `gemini-3.6-flash`, switched from `gemini-2.5-flash` on 2026-09-19 after that model started 404ing for this account; see [EVALUATION.md](EVALUATION.md)). |
| `GEMINI_EMBEDDING_MODEL` | Embedding model for RAG (default `gemini-embedding-001`). |
| `ENV` | Environment label (default `dev`). |


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


## Running the services

Two long-running processes. Run each in its own terminal:

```bash
# Control plane (REST API): interactive docs at http://localhost:8000/docs
uvicorn control_plane.app.main:app --reload

# Worker (queue consumer + executor + agent)
python -m worker.app.main
```

## Indexing runbooks

The RAG store must be seeded before the agent can ground recommendations in operational knowledge. Re-run the indexer any time a runbook is added, edited, or removed; it fully replaces all runbook rows idempotently:

```bash
python -m worker.app.agent.index_runbooks
```

Past-incident rows are indexed automatically by the agent after each failed run; no manual step is needed for those.
