# Roadmap

What is already built is in the README. Next steps:


- [ ] **Recovery-planning prompt fix for schema drift vs. data quality**: the eval (see [EVALUATION.md](EVALUATION.md)) found the model reaches for `schema_evolution` on cases that are data-quality problems (nulls, corrupt files, a muddied failure trail), not safe-to-evolve schema drift. Needs an explicit distinction in the prompt, then a re-run of the eval to confirm it moved the number.
- [ ] **Retrieval grounding weight in recovery planning**: the ablation found a past-incident chunk describing a directly relevant prior outage didn't change the recommendation. Worth more than 3 ablation cases before concluding this is systemic, but worth watching.
- [ ] **`log_analysis_node` evaluation**: the current harness doesn't score it (open-ended prose has no crisp gold label); would need an LLM-as-judge rubric instead of exact-match scoring.
- [ ] **Make `partial_load` reachable**: `run_load`'s except clause doesn't currently distinguish "wrote 0 rows" from "wrote some rows then failed," so the classifier has no signal to produce that category in production, only in the eval's synthetic cases.
- [ ] **Recommendation status sync into RAG**: update `incident_embeddings` metadata when a recommendation is applied/dismissed (currently a known TODO; retrieval falls back to similarity-only ranking until then).
- [ ] **`Literal` type constraints** retrofitted across all API schemas (batch refactor).
- [ ] **Durable checkpointing**: swap the agent's `MemorySaver` for `PostgresSaver` if/when human-in-the-loop pauses are introduced.
- [ ] **Conditional graph routing**: earned branching (e.g. low-confidence -> skip straight to escalate) once there's a concrete reason for it.
