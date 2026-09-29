# Pipeline failures

**Alerts:** `PipelineFailureRateHigh`, `CircuitBreakersOpening` (ticket)

More than half of recent pipeline runs are failing, or several pipelines' circuit breakers opened at once. Neither is an objective, because most failures come from a customer's data source and not from the platform. The job here is to tell which it is.

## Is it the platform or the data?

Group the recent failures by what went wrong:

```sql
SELECT error_type, count(*) FROM pipeline_runs
WHERE status = 'failed' AND created_at > now() - interval '1 hour'
GROUP BY error_type ORDER BY count(*) DESC;
```

- One `error_type` from one pipeline or tenant is a data source problem. Check its source URL and credentials.
- Many pipelines failing the same way at once is more likely the platform: the data warehouse database, Redis, or the network. The worker logs `pipeline_run_failed` with the error type for each one.

## What the diagnostic agent thinks

Each failed run gets a classification (`network`, `quota`, `schema`, `partial_load`, `unknown`) and a recommended action:

```sql
SELECT failure_classification, recommended_action, count(*)
FROM agent_recommendations
WHERE created_at > now() - interval '1 hour'
GROUP BY 1, 2 ORDER BY 3 DESC;
```

The recommendations are for a person to apply. The agent never acts on them itself. Mostly `unknown` means the agent is guessing, and the failures need a human read.

## Circuit breakers

A breaker opens after repeated failures on one pipeline and blocks its runs, which show up with status `blocked` and error type `CircuitBreakerOpen`. The worker logs `circuit_breaker_opened` with the pipeline id.

A breaker does not reset itself. Nothing in the worker moves it out of `open`, so a person has to do it once the cause is fixed. Set it to `half-open` through the control plane, which lets the next run through as a trial:

```bash
curl -X PUT http://localhost:8000/tenants/1/pipelines/2/circuit_breakers/ \
  -H "X-Api-Key: $TENANT_API_KEY" -H "Content-Type: application/json" \
  -d '{"state": "half-open"}'
```

If that run succeeds the worker closes the breaker (`circuit_breaker_closed`). If it fails the breaker opens again straight away. If several breakers opened at once, fix the shared cause first, then reset each one.

## When it is the platform

Follow `service-down.md` for a stopped worker, and check the data warehouse database, since the `load` step writes to it.
