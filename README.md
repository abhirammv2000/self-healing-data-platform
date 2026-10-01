# Self-Healing Data Pipeline Platform

[![CI](https://github.com/abhirammv2000/self-healing-data-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/abhirammv2000/self-healing-data-platform/actions/workflows/ci.yml)

A multi-tenant platform that runs data pipelines. When a run fails, an LLM agent works out why and recommends what to do next. A person decides whether to apply it.

"Self-healing" here means two automatic things: retries with backoff, and circuit breakers that stop a failing pipeline from running again and again. The agent only diagnoses and recommends. It never runs a fix by itself. That is on purpose.

This was a group project. Anirudh Kakati wrote most of the control plane, the worker and the diagnostic agent. Abhiram M V added the agent's tool calling, the AWS EKS setup (Terraform and Helm), the observability and SLO alerts, the unit tests, CI and the evaluation harness.

## How it works

```
 client --> control plane (FastAPI) --> Postgres <-- worker (queue consumer)
                    |                                    |
                    +------ run_id on a Redis queue -----+
                                                         |
                          failed run --> diagnostic agent (LangGraph + RAG)
                                                         |
                                                 recommendation (status: pending)
```

- **Control plane:** a REST API for tenants, pipelines, steps, schedules and runs. It creates runs and puts their ids on a Redis queue.
- **Worker:** takes runs off the queue, runs the steps with retries, and checks the circuit breaker. It writes a record of every run and sends a webhook when the run ends.
- **Diagnostic agent:** when a run fails, a LangGraph graph reads the failure, classifies it, finds related runbook sections and past incidents in pgvector, and writes a recommended action.
- **Tenancy and auth:** every query is scoped to a tenant. An admin secret creates tenants, and each tenant gets hashed API keys.

More detail is in [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## What has been tested

- **63 unit tests**, run in CI on every push. They need no database, Redis or API key.
- **A 33-case evaluation** of the agent, using hand-labeled failures and live Gemini calls: 90.9% classification accuracy and 84.8% recommended-action accuracy. The inputs to the later graph steps are hand-written stand-ins, and one weak spot is documented. See [docs/EVALUATION.md](docs/EVALUATION.md).
- **3 SLOs** with burn-rate alerts, Alertmanager, a Grafana dashboard and a runbook for each alert. The alert rules have unit tests that run in CI. The thresholds have not been tuned on real traffic. See [docs/OBSERVABILITY.md](docs/OBSERVABILITY.md) and [docs/SLOs.md](docs/SLOs.md).
- **A real AWS EKS deployment:** Terraform builds the VPC, EKS, RDS, ElastiCache and ECR. Helm installs the control plane and worker. I ran a pipeline through it, then tore everything down and checked the AWS console for leftovers. Nothing runs by default. The steps to bring it back are in [helm/shdp/README.md](helm/shdp/README.md).

## Quick start

You need Python 3.11, Docker and a Gemini API key.

```bash
python -m venv .venv && source .venv/bin/activate     # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env                                   # fill in the values
docker compose up -d                                   # Postgres and Redis
alembic upgrade head
python -m worker.app.agent.index_runbooks              # loads the runbooks for RAG

uvicorn control_plane.app.main:app --reload            # API docs at http://localhost:8000/docs
python -m worker.app.main                              # worker, in a second terminal
```

Run the tests with `pip install -r requirements-dev.txt` and `pytest tests`. All commands run from the project root. More setup detail is in [docs/SETUP.md](docs/SETUP.md).

## Layout

```
control_plane/   FastAPI API: models, routes, schemas, services
worker/          queue consumer, step handlers and the diagnostic agent (worker/app/agent)
shared/          database, Redis, config, embeddings, webhooks, observability
alembic/         migrations
eval/            the 33-case agent evaluation
infra/aws/       Terraform for the EKS deployment
helm/shdp/       Helm chart
observability/   Prometheus rules, Alertmanager, Grafana
docs/            architecture, setup, evaluation, SLOs, runbooks
```

## Limits

- The agent only recommends. Nothing applies a fix automatically.
- The evaluation is small. It also found a real problem: the agent tends to pick `schema_evolution` for data quality issues. The roadmap in [docs/ROADMAP.md](docs/ROADMAP.md) lists this and the other next steps.
- The step handlers cannot yet produce the `partial_load` failure type in real runs, only in the evaluation cases.
- Gemini is the model provider because the project first planned to run on GCP. It was never changed.
