"""Break worker/app/executor.py in 17 specific ways and check that the executor tests notice each one.

A test suite that passes proves little if it would also pass with a bug in the code. This breaks the executor on
purpose (a backoff delay off by one, a breaker that opens a failure too late, a run marked with the wrong status,
and so on), runs tests/test_executor_run.py and tests/test_executor_helpers.py, and expects at least one test to fail
each time. The file is put back byte for byte afterwards, even if this script is interrupted.

Run it from the project root:  python scripts/mutation_check_executor.py
It exits with 1 if any change goes unnoticed, or if the executor was changed and a pattern below no longer matches
(update the patterns when the executor is edited).
"""
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
TARGET = REPO / "worker" / "app" / "executor.py"
TESTS = ["tests/test_executor_run.py", "tests/test_executor_helpers.py"]

# (what is broken, the exact text to replace, the text to put there)
MUTATIONS = [
    ("backoff exponent off by one",
     "return base_delay * (2 ** (attempt - 1))\n    elif", "return base_delay * (2 ** attempt)\n    elif"),
    ("last attempt waits too",
     "delay = 0 if is_final_attempt else calculate_delay(strategy, base_delay, attempt)",
     "delay = calculate_delay(strategy, base_delay, attempt)"),
    ("breaker opens one failure too late",
     "if failed_runs_count >= circuit_breaker.failure_count_threshold:",
     "if failed_runs_count > circuit_breaker.failure_count_threshold:"),
    ("half-open success no longer closes",
     'if circuit_breaker and circuit_breaker.state == "half-open":',
     'if circuit_breaker and circuit_breaker.state == "never":'),
    ("half-open failure no longer reopens",
     'if circuit_breaker.state == "half-open":\n            # the one trial run failed',
     'if circuit_breaker.state == "never":\n            # the one trial run failed'),
    ("open breaker no longer blocks",
     'if circuit_breaker and circuit_breaker.state == "open":',
     'if circuit_breaker and circuit_breaker.state == "never":'),
    ("open breaker never recovers by itself",
     "if not await _half_open_if_due(session, circuit_breaker):", "if True:"),
    ("a run that lost the race to make the breaker half-open is blocked",
     'return circuit_breaker.state != "open"', "return result.rowcount == 1"),
    ("failure error is swallowed",
     "raise  # the original error goes to the caller", "pass"),
    ("retry count is not recorded",
     "pipeline_run.retry_count += 1", "pipeline_run.retry_count += 0"),
    ("agent failure breaks the run",
     'except Exception as error:\n        log.error("diagnostic_agent_failed"',
     'except KeyboardInterrupt as error:\n        log.error("diagnostic_agent_failed"'),
    ("claim sets the wrong status",
     '.values(\n            status="running",',
     '.values(\n            status="success",'),
    ("claim ignores the queued check",
     '.where(PipelineRun.id == run_id, PipelineRun.status == "queued")\n        .values(\n            status="running",',
     '.where(PipelineRun.id == run_id)\n        .values(\n            status="running",'),
    ("failed run not marked failed",
     'pipeline_run.status = "failed"', 'pipeline_run.status = "success"'),
    ("success not marked",
     'pipeline_run.status = "success"\n    pipeline_run.ended_at = now',
     'pipeline_run.status = "failed"\n    pipeline_run.ended_at = now'),
    ("webhook sent without the recommendation",
     '                    run_context["run_status"],\n                    recommendation_id,',
     '                    run_context["run_status"],\n                    None,'),
    ("run record failure breaks the run",
     "except ObservabilityRecordingError as error:", "except KeyboardInterrupt as error:"),
]


def main() -> int:
    original = TARGET.read_bytes()
    text = original.decode("utf-8")
    not_caught = []
    try:
        for name, old, new in MUTATIONS:
            if text.count(old) != 1:
                print(f"NO MATCH  {name}: the text to replace was found {text.count(old)} times")
                not_caught.append(name)
                continue
            TARGET.write_bytes(text.replace(old, new).encode("utf-8"))
            run = subprocess.run([sys.executable, "-m", "pytest", *TESTS, "-q", "-x", "-p", "no:warnings"],
                                 cwd=REPO, capture_output=True, text=True)
            caught = run.returncode != 0
            print(("caught    " if caught else "NOT CAUGHT  ") + name)
            if not caught:
                not_caught.append(name)
    finally:
        TARGET.write_bytes(original)
    if not_caught:
        print(f"\n{len(not_caught)} of {len(MUTATIONS)} changes were not caught: {not_caught}")
        return 1
    print(f"\nall {len(MUTATIONS)} changes were caught")
    return 0


if __name__ == "__main__":
    sys.exit(main())
