# Service down

**Alerts:** `ControlPlaneDown` (page, after 1 minute), `WorkerDown` (page, after 2 minutes)

Prometheus can't reach the process's `/metrics`. Either the process is down or Prometheus can't get to it. The availability ratios can't see this, since a service that serves nothing sends no failing requests.

## Control plane

1. Is it answering? `curl -i http://localhost:8000/health`. In Kubernetes: `kubectl get pods -l app=shdp-control-plane` (the pods carry that `app` label) and `kubectl logs deploy/shdp-control-plane --tail=100`.
2. If it is up but Prometheus can't reach it, look at Status, Targets in Prometheus (`:9090`). Locally Prometheus reaches the control plane at `host.docker.internal:8000`, so a control plane that binds only to `127.0.0.1` is invisible to it. Start it with `--host 0.0.0.0`.
3. If it crashed on startup, the usual causes are the database being unreachable (check `docker compose ps postgres`) or a missing migration (`alembic upgrade head`).

## Worker

1. Is it running? Locally `python -m worker.app.main`; in Kubernetes `kubectl logs deploy/shdp-worker --tail=100`. A healthy worker logs `worker_started` and then `run_picked_up` for each run.
2. Its metrics are on port 8001, so `curl http://localhost:8001/metrics` tells you if the process is alive but not scraped.
3. While it is down, runs pile up in the Redis queue. Check how many: `docker compose exec redis redis-cli LLEN pipeline_runs`. They are picked up in order once the worker is back, so nothing is lost, only delayed. Cron schedules start no runs while every worker is down; when one is back it starts one run for the newest missed time.

## After it is back

Confirm both targets show `UP` in Prometheus and the alert resolves. If a worker died in the middle of a run, another worker takes the run over once its lease has run out (60 s by default) and starts it again from the first step. The worker logs `run_requeued_worker_lost`, and `pipeline_runs_recovered_total` goes up. A run that loses its worker `RUN_MAX_ATTEMPTS` times (3) is failed with `error_type = worker_lost`. To check for runs that are still stuck:

```sql
SELECT id, pipeline_id, started_at FROM pipeline_runs
WHERE status = 'running' AND lease_expires_at < now();
```

That query should return nothing for more than a minute or so while a worker is up. If it keeps returning rows, no worker is running the reaper: see [RELIABILITY.md](../RELIABILITY.md).
