# API availability

**Alerts:** `ApiAvailabilityFastBurn`, `ApiAvailabilitySlowBurn` (page), `ApiAvailabilityBudgetLeak`, `ApiAvailabilityBudgetDrift` (ticket)

The control plane is returning 5xx responses faster than the 99.5% objective allows. The fast alert means a lot of requests are failing right now. The slower ones mean a lower error rate that has been going on for hours or days.

## First checks

1. Which endpoints? In Prometheus:

   ```
   sum by (handler) (rate(http_requests_total{job="control-plane", status="5xx"}[5m]))
   ```

   One handler points to a bug in that route. Every handler points to something shared: the database, or the process itself.

2. Errors in the logs. The control plane logs structured JSON, so filter for `"level": "error"`. Every request and database call is also a span in Jaeger (`:16686`), and log lines carry the trace id.

3. The database. `docker compose ps postgres`, then connect:

   ```bash
   docker compose exec postgres psql -U postgres -d data_platform_db -c "select count(*) from pipeline_runs;"
   ```

   If that hangs or fails, the control plane is failing because of it.

## Causes seen so far

- The database is unreachable or out of connections. Restart Postgres or the control plane.
- A migration hasn't been applied after a deploy (`alembic upgrade head`).
- A bad deploy of one route. Roll back with `helm rollback shdp` if it is on Kubernetes.

## Slow burns

`BudgetLeak` and `BudgetDrift` are tickets, not pages. A low steady error rate is usually one route that always fails for one kind of input. The handler query above finds it. Fix it during working hours.

## Error budget

If the budget is spent, stop feature work on the control plane until it is back under budget (see `docs/SLOs.md`).
