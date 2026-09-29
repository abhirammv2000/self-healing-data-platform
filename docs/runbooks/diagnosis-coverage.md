# Diagnosis coverage

**Alert:** `DiagnosisCoverageLow` (ticket)

Failed runs are not getting a recommendation. The objective is 95% covered, and this alert fires when more than 30% of failed runs over 6 hours have none, with at least 5 failures in that window.

The diagnostic agent is built to never break the executor. If it fails, the run still finishes and the failure is logged, so a gap here is quiet unless you look.

## First checks

1. Errors from the agent. The worker logs `diagnostic_agent_failed` with the reason. Locally, look in the terminal running `python -m worker.app.main`. On Kubernetes:

   ```bash
   kubectl logs deploy/shdp-worker | grep diagnostic_agent_failed
   ```

2. Which failed runs have no recommendation:

   ```sql
   SELECT r.id, r.pipeline_id, r.error_type, r.created_at
   FROM pipeline_runs r
   LEFT JOIN agent_recommendations a ON a.run_id = r.id
   WHERE r.status = 'failed' AND a.id IS NULL
     AND r.created_at > now() - interval '6 hours'
   ORDER BY r.created_at DESC;
   ```

## Usual causes

- The model API key is missing, expired or out of quota. The agent calls Gemini, and a rejected key fails every call.
- The model name is wrong or has been retired. Earlier the default `gemini-2.5-flash` started returning 404 for this account, and the default had to change. Check `GEMINI_MODEL`.
- The pgvector database is unreachable, so retrieval fails. The agent is meant to degrade to a sentinel recommendation in that case, so a full gap points at the model call.

## After the fix

Old failed runs are not diagnosed retroactively. The agent runs when a run fails. To get a recommendation for one that was missed, the run has to fail again.

## About this alert

Failures are rare, so this indicator has little traffic and one bad hour can swing it. That is why it is a ticket and has the minimum of 5 failed runs.
