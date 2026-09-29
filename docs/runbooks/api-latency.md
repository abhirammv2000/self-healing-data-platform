# API latency

**Alerts:** `ApiLatencyFastBurn`, `ApiLatencySlowBurn` (page), `ApiLatencyBudgetLeak`, `ApiLatencyBudgetDrift` (ticket)

Too many control plane requests are taking longer than 0.5 seconds. The objective allows 5%.

## First checks

1. Which endpoints are slow? The share of requests slower than 0.5s, by handler:

   ```
   1 - (
     sum by (handler) (rate(http_request_duration_seconds_bucket{job="control-plane", le="0.5"}[15m]))
     /
     sum by (handler) (rate(http_request_duration_seconds_count{job="control-plane"}[15m]))
   )
   ```

2. Where does the time go inside a slow request? Open Jaeger (`:16686`), pick a slow trace for that handler, and look at the SQLAlchemy spans. The control plane is mostly database calls, so a slow request is usually a slow query.

3. Database load. Long-running queries:

   ```sql
   SELECT pid, now() - query_start AS running, left(query, 80)
   FROM pg_stat_activity WHERE state = 'active' ORDER BY running DESC LIMIT 5;
   ```

## Causes to look for

- A list endpoint without a `limit` on a large table. `GET /tenants/{id}/runs` accepts `limit` and `offset`, and a caller that leaves them off gets every row.
- The worker and control plane sharing a database that is short on CPU while a burst of runs executes.
- A missing index after a schema change.

## Slow burns

The ticket alerts mean a smaller share of slow requests over a longer time. The by-handler query above usually finds one route to fix.

## Reading the numbers

The instrumentator's default buckets are 0.1s, 0.5s and 1s. The 95th percentile on the dashboard is an estimate from those, so it says which bucket the answer falls in and no more.
