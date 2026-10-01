# Testing and evaluation

**Automated: 63 unit tests, run in CI on every push.**

```bash
pip install -r requirements-dev.txt
pytest tests -v
```

No Postgres, pgvector, Redis, or `GOOGLE_API_KEY` is needed to run the suite; every network and DB boundary is mocked, and `conftest.py` sets fake env vars before any app module is imported (several build a DB engine or an LLM client at import time). Coverage:

- `worker/app/step_handlers.py`: every reachable failure mode of the four pipeline steps (missing `source_url`, a failed fetch, missing/malformed CSV, missing columns, null values, too few rows, an unknown filter column, an unsupported filter operator, a filter-value type mismatch, a data-warehouse write failure), plus each step's success path.
- `worker/app/agent/nodes.py`: each LangGraph node's success path and its fall-back-to-sentinel path on an LLM failure, and that `classification_node`/`recovery_planning_node` skip the LLM call entirely (not just degrade) when their required upstream input is missing.
- `worker/app/agent/retrieval_node.py`: query construction (including the defensive all-`None` branch), combining and globally re-sorting the two pgvector queries by similarity, and swallowing an embedding or DB failure into an empty context rather than crashing the graph.
- `worker/app/agent/diagnostic_agent.py`: all four documented "write nothing, return `None`" paths (non-`failed` run, unserializable `run_context`, a graph-invocation crash, a persistence failure) plus the success path, checking the persisted row's fields and that incident indexing is fired with the recommendation id.
- `worker/app/agent/state.py`: the three sentinel outputs always degrade to the safe choice (`unknown`/`0.0` confidence, `escalate`).

**Manual, still needed for anything end-to-end (a live DB/LLM run):**

- **Endpoints**: Swagger UI at `/docs`.
- **Webhooks**: [webhook.site](https://webhook.site) to inspect delivered payloads (set a pipeline's `callback_url` to a webhook.site URL).
- **Data**: direct Postgres queries to verify run records, recommendations, circuit-breaker state, and indexed embeddings.

### Agent evaluation harness

`eval/cases.py` holds 33 hand-labeled failure cases, each grounded in an exception `worker/app/step_handlers.py` actually raises (see that module's docstring for the two categories that go beyond what it can currently produce, and why). `eval/run_eval.py` runs every case through `classification_node` and `recovery_planning_node` (live Gemini calls, real cost) and scores the predictions against the gold labels. It does not exercise `log_analysis_node` or `retrieval_node`/pgvector; each case supplies a hand-authored stand-in for what those would have produced, for the reasons explained in `eval/cases.py`'s module docstring. `tests/test_eval_harness.py` checks the scoring logic itself (exact-match, alternate-answer, and ablation accounting) with mocked chains, at zero cost, separate from any run against the live API.

**Results, run 2026-09-20 against `gemini-3.6-flash`** (33 cases, 69 calls total; see `eval/results/` for the raw predictions and full metrics):

| Metric | Strict | Lenient (alternate answer accepted) |
|---|---|---|
| Classification accuracy | 90.9% | 93.9% |
| Recommended-action accuracy | 84.8% | 87.9% |
| Both correct | 75.8% | N/A |

Retrieval ablation (the 3 cases that carry a hand-authored runbook/incident chunk, each run once with it and once with it stripped to `[]`): in 1 of 3, the retrieved context measurably changed the recommended action in the expected direction (a runbook note overriding the default network response for one specific host). In the other 2, the action came out identical with or without context. One of those is a control case where that's the expected result (the runbook simply confirms the default); the other is a weakness: a past-incident chunk describing a prior outage on the same pipeline/host didn't move the model off its default `retry_with_backoff`, when the incident history arguably should have. Three cases is too small to call this a trend.

**What the confusion pairs show:** all 3 classification misses were `unknown -> {network, quota, schema}`, never the other direction. Two of the three (`-> network`, `-> quota`) are cases whose `log_analysis` carries a `log_analysis_failed` signal: the model classified off the raw error text instead of deferring to the degradation signal the way the prompt instructs. In both, the *recommended action* still came out `escalate` anyway, so the operator-visible behavior was right even though the intermediate label wasn't. The third (`-> schema`) is the typo'd-filter-column config-bug case, where "schema" is a reasonable read even though "unknown" was the gold call; that one's covered by the case's own accepted alternate.

**A pattern worth naming rather than averaging away:** 3 of the 5 strict misses on `recommended_action` (the ones with gold `escalate` and no accepted alternate) were all cases where the model instead picked `schema_evolution`: a null-value data-quality issue, a corrupt/unparseable CSV, and a case where an earlier, already-resolved quota blip in the same run muddied which failure was being classified. (A fourth, a column-type mismatch, also got `schema_evolution` over `escalate`, but that one counts as lenient-correct since the case's own gold label already flags `schema_evolution` as a defensible alternate.) The recovery-planning prompt doesn't currently draw a line between "this schema drift is safe to auto-evolve" and "this is a data-quality problem that only looks schema-shaped," so the model consistently reaches for the more automated-sounding action. That's a prompt gap, and the most concrete next step this eval surfaced.
