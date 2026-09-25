"""Runs the labeled cases in eval/cases.py through the REAL classification_node
and recovery_planning_node (real Gemini calls, real cost; see eval/cases.py's
module docstring for what this does and doesn't cover).

Usage (run from the project root, so python-dotenv finds .env there):
    python -m eval.run_eval

Requires a real GOOGLE_API_KEY in .env. Does NOT import diagnostic_agent.py or
shared/db.py, and never touches a database. Only nodes.py's two LLM nodes are
exercised directly, with each case's own hand-authored log_analysis and
retrieved_context standing in for what the earlier graph nodes would have
produced.

What this script does, per case:
  1. Run classification_node with the case's log_analysis fixture.
  2. Run recovery_planning_node with that classification + the case's own
     retrieved_context (empty for most cases).
  3. For the 3 cases in RETRIEVAL_ABLATION_CASE_IDS only, also re-run
     recovery_planning_node with retrieved_context forced to []. This is the
     ablation; nothing else is re-run twice, since classification_node never
     reads retrieved_context and re-running it would just burn calls to get
     the same answer twice.

Scoring is against gold_classification / gold_action strictly, and separately
against the acceptable_alternate_* fields leniently, both reported. See
eval/cases.py's EvalCase docstring for why a few cases carry an alternate.
"""
import asyncio
import json
import sys
from collections import Counter
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from eval.cases import CASES, RETRIEVAL_ABLATION_CASE_IDS, EvalCase, RetrievedChunkFixture
from worker.app.agent.nodes import classification_node, recovery_planning_node
from worker.app.agent.schemas import LogAnalysisOutput
from worker.app.agent.state import RetrievedChunk


def to_log_analysis_output(fixture) -> LogAnalysisOutput:
    return LogAnalysisOutput(
        failed_step=fixture.failed_step,
        attempt_pattern=fixture.attempt_pattern,
        error_interpretation=fixture.error_interpretation,
        notable_signals=fixture.notable_signals,
    )


def to_retrieved_chunks(fixtures: list[RetrievedChunkFixture]) -> list[RetrievedChunk]:
    return [
        RetrievedChunk(source_type=f.source_type, source_ref=f.source_ref, chunk_text=f.chunk_text, similarity_score=f.similarity_score)
        for f in fixtures
    ]


async def run_case(case: EvalCase) -> dict:
    log_analysis = to_log_analysis_output(case.log_analysis)

    classification_result = await classification_node({
        "run_id": case.id, "error_type": case.error_type, "error_message": case.error_message,
        "log_analysis": log_analysis,
    })
    classification = classification_result["classification"]

    recovery_result = await recovery_planning_node({
        "run_id": case.id, "error_type": case.error_type, "error_message": case.error_message,
        "log_analysis": log_analysis, "classification": classification,
        "retrieved_context": to_retrieved_chunks(case.retrieved_context),
    })
    recovery_plan = recovery_result["recovery_plan"]

    record = {
        "id": case.id,
        "gold_classification": case.gold_classification,
        "predicted_classification": classification.failure_classification,
        "classification_confidence": classification.confidence,
        "acceptable_alternate_classification": case.acceptable_alternate_classification,
        "gold_action": case.gold_action,
        "predicted_action": recovery_plan.recommended_action,
        "acceptable_alternate_action": case.acceptable_alternate_action,
        "explanation": recovery_plan.explanation,
    }

    if case.id in RETRIEVAL_ABLATION_CASE_IDS:
        ablation_result = await recovery_planning_node({
            "run_id": case.id, "error_type": case.error_type, "error_message": case.error_message,
            "log_analysis": log_analysis, "classification": classification,
            "retrieved_context": [],
        })
        ablation_plan = ablation_result["recovery_plan"]
        record["ablation"] = {
            "action_without_context": ablation_plan.recommended_action,
            "expected_action_without_context": case.expected_action_without_context,
            "action_changed_vs_with_context": ablation_plan.recommended_action != recovery_plan.recommended_action,
            "matches_expected_fallback": ablation_plan.recommended_action == case.expected_action_without_context,
        }

    return record


def score(records: list[dict]) -> dict:
    n = len(records)

    def is_correct(field_pred, field_gold, field_alt):
        return lambda r: r[field_pred] == r[field_gold] or (r[field_alt] is not None and r[field_pred] == r[field_alt])

    def is_strict(field_pred, field_gold):
        return lambda r: r[field_pred] == r[field_gold]

    classification_strict = sum(1 for r in records if is_strict("predicted_classification", "gold_classification")(r))
    classification_lenient = sum(1 for r in records if is_correct("predicted_classification", "gold_classification", "acceptable_alternate_classification")(r))
    action_strict = sum(1 for r in records if is_strict("predicted_action", "gold_action")(r))
    action_lenient = sum(1 for r in records if is_correct("predicted_action", "gold_action", "acceptable_alternate_action")(r))
    both_strict = sum(1 for r in records if is_strict("predicted_classification", "gold_classification")(r) and is_strict("predicted_action", "gold_action")(r))

    confusion = Counter(
        (r["gold_classification"], r["predicted_classification"])
        for r in records if r["predicted_classification"] != r["gold_classification"]
    )

    ablation_records = [r for r in records if "ablation" in r]

    return {
        "n_cases": n,
        "classification_accuracy_strict": round(classification_strict / n, 3),
        "classification_accuracy_lenient": round(classification_lenient / n, 3),
        "action_accuracy_strict": round(action_strict / n, 3),
        "action_accuracy_lenient": round(action_lenient / n, 3),
        "both_correct_strict": round(both_strict / n, 3),
        "classification_confusion_pairs (gold -> predicted)": {f"{g} -> {p}": c for (g, p), c in confusion.most_common()},
        "retrieval_ablation": [
            {
                "id": r["id"],
                "action_with_context": r["predicted_action"],
                "action_without_context": r["ablation"]["action_without_context"],
                "changed": r["ablation"]["action_changed_vs_with_context"],
                "matched_expected_fallback": r["ablation"]["matches_expected_fallback"],
            }
            for r in ablation_records
        ],
    }


async def main():
    print(f"Running {len(CASES)} cases against the real classification_node and recovery_planning_node...")
    print(f"({len(CASES) + len(RETRIEVAL_ABLATION_CASE_IDS)} total Gemini calls: {len(CASES)} classification + {len(CASES)} recovery-planning + {len(RETRIEVAL_ABLATION_CASE_IDS)} ablation re-runs)\n")

    records = []
    for i, case in enumerate(CASES, start=1):
        print(f"[{i}/{len(CASES)}] {case.id} ...", end=" ", flush=True)
        record = await run_case(case)
        match = "OK" if record["predicted_classification"] == case.gold_classification and record["predicted_action"] == case.gold_action else "MISMATCH"
        print(f"{match} (classification={record['predicted_classification']}, action={record['predicted_action']})")
        records.append(record)

    metrics = score(records)

    out_dir = Path(__file__).resolve().parent / "results"
    out_dir.mkdir(exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    raw_path = out_dir / f"{timestamp}_raw.json"
    raw_path.write_text(json.dumps(records, indent=2), encoding="utf-8")

    metrics_path = out_dir / f"{timestamp}_metrics.json"
    metrics_path.write_text(json.dumps(metrics, indent=2), encoding="utf-8")

    print("\n" + "=" * 70)
    print(json.dumps(metrics, indent=2))
    print("=" * 70)
    print(f"\nRaw predictions: {raw_path}")
    print(f"Metrics: {metrics_path}")


if __name__ == "__main__":
    asyncio.run(main())
