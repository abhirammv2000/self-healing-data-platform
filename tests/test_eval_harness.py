"""Zero-cost sanity check on eval/run_eval.py's own scoring logic, run before
ever pointing it at a Gemini key. Same FakeChain mocking approach as
tests/agent/test_nodes.py. This isn't testing the model's accuracy (that's
the point of the real run); it's testing that run_eval.py counts correctly:
exact matches, alternates, confusion pairs, and the retrieval ablation's
fallback comparison.
"""
from unittest.mock import AsyncMock

from eval import run_eval
from eval.cases import CASES, RETRIEVAL_ABLATION_CASE_IDS
from worker.app.agent import nodes
from worker.app.agent.schemas import ClassificationOutput, RecoveryPlanOutput


class FakeChain:
    def __init__(self, ainvoke_mock):
        self.ainvoke = ainvoke_mock


async def test_perfect_predictions_score_100_percent(monkeypatch):
    """If the model always outputs exactly the gold label, every metric should read 1.0
    and there should be zero confusion pairs; the harness shouldn't invent errors."""

    async def fake_classify(inputs):
        # inputs dict passed to the chain has the case's error_type/error_message etc,
        # but not the gold label, so look it up by matching on error_message,
        # which is unique per case in eval/cases.py.
        case = next(c for c in CASES if c.error_message == inputs["error_message"])
        return ClassificationOutput(failure_classification=case.gold_classification, confidence=0.9, reasoning="x" * 25)

    async def fake_recover(inputs):
        case = next(c for c in CASES if c.error_message == inputs["error_message"])
        return RecoveryPlanOutput(recommended_action=case.gold_action, explanation="y" * 25)

    monkeypatch.setattr(nodes, "_classification_chain", FakeChain(AsyncMock(side_effect=fake_classify)))
    monkeypatch.setattr(nodes, "_recovery_planning_chain", FakeChain(AsyncMock(side_effect=fake_recover)))

    records = [await run_eval.run_case(case) for case in CASES]
    metrics = run_eval.score(records)

    assert metrics["classification_accuracy_strict"] == 1.0
    assert metrics["action_accuracy_strict"] == 1.0
    assert metrics["both_correct_strict"] == 1.0
    assert metrics["classification_confusion_pairs (gold -> predicted)"] == {}


async def test_wrong_predictions_are_caught_and_alternate_scoring_works(monkeypatch):
    """A model that always says 'unknown'/'escalate' should score 0% strict overall,
    except on the handful of cases whose gold label is already unknown/escalate.
    Lenient accuracy should be at least as high as strict, never lower."""

    async def always_unknown(inputs):
        return ClassificationOutput(failure_classification="unknown", confidence=0.1, reasoning="x" * 25)

    async def always_escalate(inputs):
        return RecoveryPlanOutput(recommended_action="escalate", explanation="y" * 25)

    monkeypatch.setattr(nodes, "_classification_chain", FakeChain(AsyncMock(side_effect=always_unknown)))
    monkeypatch.setattr(nodes, "_recovery_planning_chain", FakeChain(AsyncMock(side_effect=always_escalate)))

    records = [await run_eval.run_case(case) for case in CASES]
    metrics = run_eval.score(records)

    expected_classification_accuracy = sum(1 for c in CASES if c.gold_classification == "unknown") / len(CASES)
    expected_action_accuracy = sum(1 for c in CASES if c.gold_action == "escalate") / len(CASES)

    assert metrics["classification_accuracy_strict"] == round(expected_classification_accuracy, 3)
    assert metrics["action_accuracy_strict"] == round(expected_action_accuracy, 3)
    assert metrics["classification_accuracy_lenient"] >= metrics["classification_accuracy_strict"]
    assert metrics["action_accuracy_lenient"] >= metrics["action_accuracy_strict"]
    assert len(metrics["classification_confusion_pairs (gold -> predicted)"]) > 0


async def test_ablation_records_are_only_produced_for_the_three_ablation_cases(monkeypatch):
    async def fake_classify(inputs):
        case = next(c for c in CASES if c.error_message == inputs["error_message"])
        return ClassificationOutput(failure_classification=case.gold_classification, confidence=0.9, reasoning="x" * 25)

    async def fake_recover(inputs):
        case = next(c for c in CASES if c.error_message == inputs["error_message"])
        # with-context call gets retrieved_context_block populated when a chunk was given;
        # the ablation re-run always passes an empty list, which renders the placeholder text.
        if inputs["retrieved_context_block"] == "No relevant prior incidents or runbooks were retrieved for this failure.":
            return RecoveryPlanOutput(recommended_action=case.expected_action_without_context or case.gold_action, explanation="y" * 25)
        return RecoveryPlanOutput(recommended_action=case.gold_action, explanation="y" * 25)

    monkeypatch.setattr(nodes, "_classification_chain", FakeChain(AsyncMock(side_effect=fake_classify)))
    monkeypatch.setattr(nodes, "_recovery_planning_chain", FakeChain(AsyncMock(side_effect=fake_recover)))

    records = [await run_eval.run_case(case) for case in CASES]

    ablation_ids = {r["id"] for r in records if "ablation" in r}
    assert ablation_ids == set(RETRIEVAL_ABLATION_CASE_IDS)

    metrics = run_eval.score(records)
    for row in metrics["retrieval_ablation"]:
        assert row["matched_expected_fallback"] is True
