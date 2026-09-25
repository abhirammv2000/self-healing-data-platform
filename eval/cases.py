"""Labeled failure cases for the diagnostic agent's classification and
recovery-planning eval (see eval/run_eval.py).

Scope: this evaluates classification_node and recovery_planning_node only,
not log_analysis_node. Those two have categorical outputs that can be scored
against a gold label, and they drive the automated behavior an operator sees
(the classification feeds the recommendation, which an operator applies or
dismisses). log_analysis_node's output is open-ended prose with no crisp gold
label to score, so each case below supplies a hand-authored LogAnalysisOutput
fixture standing in for it, the way a unit test supplies a fixture instead of
exercising the whole pipeline. Scoring log_analysis's own descriptive quality
would need a different method (an LLM-as-judge rubric) and is future work.

This also can't run the real retrieval_node against pgvector, since no
incident/runbook corpus is indexed in this environment. Each case instead
supplies its own hand-authored `retrieved_context`, empty for most cases,
with a specific runbook or past-incident chunk for the cases in
RETRIEVAL_ABLATION_CASE_IDS. run_eval.py runs every case twice, once with its
given retrieved_context and once with an empty list. If retrieval earns its
place in the graph, the ablation cases should show a measurable accuracy or
grounding difference between the two runs; the rest of the cases are the
control group and shouldn't move much either way.

Every error_type/error_message pattern below is grounded in an exception this
codebase actually raises; see worker/app/step_handlers.py and the httpx call
in run_ingestion's fetch_data(). Two categories go beyond what
step_handlers.py can currently produce, noted per case below:

- partial_load cases assume a load-step error message granular enough to say
  how many rows were written before the failure. run_load's current except
  clause catches any exception from df.to_sql() uniformly and never
  distinguishes "wrote 0 rows" from "wrote some rows then failed partway," so
  in production today the classifier has no evidence to ever reach
  partial_load. These cases test whether the model can recognize the
  category given the right signal, ahead of the separate work of making
  step_handlers.py produce that signal.
- the two "config bug" cases (unknown source_url, missing ingestion output)
  are pipeline-definition errors, not data failures. None of the five
  classification categories fit them well; that gap is itself the finding.
  The taxonomy assumes something went wrong with the data, not that the
  pipeline was misconfigured.
"""
from dataclasses import dataclass, field
from typing import Literal


@dataclass
class LogAnalysisFixture:
    failed_step: str
    attempt_pattern: str
    error_interpretation: str
    notable_signals: list[str] = field(default_factory=list)


@dataclass
class RetrievedChunkFixture:
    source_type: Literal["past_run", "past_recommendation", "runbook"]
    source_ref: str
    chunk_text: str
    similarity_score: float


@dataclass
class EvalCase:
    id: str
    error_type: str
    error_message: str
    log_analysis: LogAnalysisFixture
    gold_classification: str
    gold_action: str
    notes: str
    retrieved_context: list[RetrievedChunkFixture] = field(default_factory=list)
    # Alternates for cases whose notes call out a defensible second answer.
    # Lets run_eval.py score "strict" (only gold_* counts) and "lenient"
    # (alternate also counts) accuracy separately instead of eyeballing free
    # text after the numbers are already in. None means no alternate is accepted.
    acceptable_alternate_classification: str | None = None
    acceptable_alternate_action: str | None = None
    # Only set on the 3 retrieval-ablation cases: the action a correct model
    # should fall back to when retrieved_context is stripped to []. Scores the
    # "without context" run against something concrete instead of eyeballing
    # whether the answer changed.
    expected_action_without_context: str | None = None


def la(failed_step, attempt_pattern, error_interpretation, notable_signals=None):
    return LogAnalysisFixture(failed_step, attempt_pattern, error_interpretation, notable_signals or [])


def chunk(source_type, source_ref, text, score):
    return RetrievedChunkFixture(source_type, source_ref, text, score)


CASES: list[EvalCase] = [

    # ---- network: transient (timeouts, resets, 5xx) -> retry_with_backoff ----
    EvalCase(
        id="net-connect-timeout-first-attempt",
        error_type="IngestionStepError",
        error_message="Failed to fetch from https://partner-feed.example.com/daily.csv: connection timed out after 30s",
        log_analysis=la("ingestion", "failed on first attempt with no retries configured",
                         "a connection timeout while fetching from an external HTTP source"),
        gold_classification="network", gold_action="retry_with_backoff",
        notes="Single timeout, one attempt, no prior pattern: transient by every available signal, matching the prompt's own guidance for timeouts.",
    ),
    EvalCase(
        id="net-read-timeout-mid-download",
        error_type="IngestionStepError",
        error_message="Failed to fetch from https://data.vendor.io/exports/2026-09-17.csv: read timed out while streaming response body",
        log_analysis=la("ingestion", "failed on first attempt with no retries configured",
                         "a read timeout partway through downloading a large file from an external source"),
        gold_classification="network", gold_action="retry_with_backoff",
        notes="A slow/overloaded upstream mid-transfer is the textbook retry_with_backoff case.",
    ),
    EvalCase(
        id="net-connection-reset-after-retries",
        error_type="IngestionStepError",
        error_message="Failed to fetch from https://api.supplier.com/v2/inventory: connection reset by peer",
        log_analysis=la("ingestion", "failed 3 times with the same connection-reset error after exponential backoff",
                         "repeated connection resets from an external HTTP source, consistent across all attempts"),
        gold_classification="network", gold_action="retry_with_backoff",
        acceptable_alternate_action="pause_schedule",
        notes="Still transient in kind, but it already exhausted its configured retries. Recommending retry_with_backoff again is defensible (a scheduled re-run later may succeed), though this is the closest network case to deserving pause_schedule instead. Flagged as a close call.",
    ),
    EvalCase(
        id="net-5xx-upstream",
        error_type="IngestionStepError",
        error_message="Failed to fetch from https://feed.retailer.com/skus.csv: 503 Service Unavailable",
        log_analysis=la("ingestion", "failed on first attempt with no retries configured",
                         "the external source returned a 503, indicating it is temporarily overloaded or down for maintenance"),
        gold_classification="network", gold_action="retry_with_backoff",
        notes="5xx is explicitly called out in the recovery prompt as the transient case.",
    ),

    # ---- network: looks permanent (4xx, DNS, bad URL) -> pause_schedule ----
    EvalCase(
        id="net-404-moved-endpoint",
        error_type="IngestionStepError",
        error_message="Failed to fetch from https://legacy.vendor.com/export/nightly.csv: 404 Not Found",
        log_analysis=la("ingestion", "failed on first attempt with no retries configured",
                         "the external source returned a 404, indicating the resource does not exist at this URL"),
        gold_classification="network", gold_action="pause_schedule",
        notes="A 404 won't fix itself on retry; the endpoint moved or was removed. Per the recovery prompt, 4xx errors should lean pause_schedule over retry_with_backoff.",
    ),
    EvalCase(
        id="net-dns-nxdomain",
        error_type="IngestionStepError",
        error_message="Failed to fetch from https://old-partner-domain.example.com/feed.csv: [Errno 11001] getaddrinfo failed, DNS lookup returned NXDOMAIN",
        log_analysis=la("ingestion", "failed on first attempt with no retries configured",
                         "the source hostname does not resolve at all, suggesting the domain is gone or was never valid"),
        gold_classification="network", gold_action="pause_schedule",
        notes="NXDOMAIN is explicitly named in the recovery prompt as a pause_schedule signal.",
    ),
    EvalCase(
        id="net-401-credentials-revoked",
        error_type="IngestionStepError",
        error_message="Failed to fetch from https://api.partner.com/v1/export: 401 Unauthorized",
        log_analysis=la("ingestion", "failed on first attempt with no retries configured",
                         "the external API rejected the request's credentials, a token was likely revoked or expired"),
        gold_classification="network", gold_action="pause_schedule",
        acceptable_alternate_action="escalate",
        notes="A retry can't fix expired credentials; someone has to rotate them. escalate is also defensible here, so this case scores partial credit if the model picks escalate instead of pause_schedule.",
    ),

    # ---- quota: 429 / throttling -> retry_with_backoff ----
    EvalCase(
        id="quota-429-explicit",
        error_type="IngestionStepError",
        error_message="Failed to fetch from https://api.ratelimited-vendor.com/data: 429 Too Many Requests",
        log_analysis=la("ingestion", "failed on first attempt with no retries configured",
                         "the external API rejected the request due to rate limiting"),
        gold_classification="quota", gold_action="retry_with_backoff",
        notes="429 is the unambiguous quota signal. Tests that the model routes it to quota rather than the more generic network bucket, since both are 'HTTP errors' under the network definition's wording.",
    ),
    EvalCase(
        id="quota-429-with-retry-after-header-mentioned",
        error_type="IngestionStepError",
        error_message="Failed to fetch from https://api.ratelimited-vendor.com/data: 429 Too Many Requests (Retry-After: 120)",
        log_analysis=la("ingestion", "failed on first attempt with no retries configured",
                         "rate limited by the external API, which specified a 120 second retry window"),
        gold_classification="quota", gold_action="retry_with_backoff",
        notes="Same as above with an explicit backoff hint present. Should not change the category, only possibly the explanation's specificity.",
    ),
    EvalCase(
        id="quota-daily-limit-exhausted",
        error_type="IngestionStepError",
        error_message="Failed to fetch from https://api.premiumdata.com/v1/pull: 403 Forbidden - daily API quota exhausted, resets at 00:00 UTC",
        log_analysis=la("ingestion", "failed on first attempt with no retries configured",
                         "the external API's daily quota has been exhausted for this account"),
        gold_classification="quota", gold_action="pause_schedule",
        notes="A quota case where retry_with_backoff is wrong: no amount of near-term backoff helps until the daily window resets, so pause_schedule (retry tomorrow) is correct. Tests that the model isn't pattern-matching 'quota == always retry_with_backoff'.",
    ),
    EvalCase(
        id="quota-throttled-mid-batch",
        error_type="IngestionStepError",
        error_message="Failed to fetch from https://api.batchvendor.io/orders: 429 Too Many Requests - concurrent request limit exceeded",
        log_analysis=la("ingestion", "failed 2 times with the same throttling error, delay increased each attempt",
                         "the external API is throttling concurrent requests from this integration"),
        gold_classification="quota", gold_action="retry_with_backoff",
        notes="Standard concurrency throttling. Backoff resolves this without waiting for a fixed reset window, unlike the daily-quota case above.",
    ),

    # ---- schema: missing/null columns -> schema_evolution when plausible, escalate when not ----
    EvalCase(
        id="schema-missing-optional-looking-column",
        error_type="ValidationStepError",
        error_message="Validation failed: Missing required columns: ['promo_code']",
        log_analysis=la("validation", "failed on first attempt, no retries configured for validation",
                         "the ingested file is missing a column the validation config expects to be present"),
        gold_classification="schema", gold_action="schema_evolution",
        notes="A single missing, plausibly optional column (a promo code) is the clearest schema_evolution case: add it as nullable and replay, per the action's own definition.",
    ),
    EvalCase(
        id="schema-missing-core-identifier-column",
        error_type="ValidationStepError",
        error_message="Validation failed: Missing required columns: ['order_id', 'customer_id']",
        log_analysis=la("validation", "failed on first attempt, no retries configured for validation",
                         "the ingested file is missing both of its primary identifier columns entirely"),
        gold_classification="schema", gold_action="escalate",
        notes="Missing both identifier columns entirely suggests a structurally different or corrupted file rather than minor schema drift. schema_evolution (add nullable columns and continue) doesn't make sense when the join keys themselves are gone.",
    ),
    EvalCase(
        id="schema-null-values-in-required-column",
        error_type="ValidationStepError",
        error_message="Validation failed: Column 'total_amount' contains null values",
        log_analysis=la("validation", "failed on first attempt, no retries configured for validation",
                         "a column expected to always be populated has null values in this run's data"),
        gold_classification="schema", gold_action="escalate",
        notes="This is a data quality problem in the source data, not a shape/schema drift; there's nothing to evolve. escalate so a human can check with the data source.",
    ),
    EvalCase(
        id="schema-row-count-below-minimum",
        error_type="ValidationStepError",
        error_message="Validation failed: CSV has 12 rows, expected at least 1000",
        log_analysis=la("validation", "failed on first attempt, no retries configured for validation",
                         "the ingested file has far fewer rows than expected, suggesting an incomplete or truncated source export"),
        gold_classification="schema", gold_action="escalate",
        notes="Row count is checked in the validation step, not the load step, so per the classifier prompt's own scoping this is 'schema' (a validation-failure shape issue), not partial_load (load-step-specific). Tests whether the model over-generalizes partial_load to any 'incomplete data' framing.",
    ),
    EvalCase(
        id="schema-corrupt-csv-unreadable",
        error_type="ValidationStepError",
        error_message="Failed to read CSV file: Error tokenizing data. C error: Expected 8 fields in line 4302, saw 11",
        log_analysis=la("validation", "failed on first attempt, no retries configured for validation",
                         "the file cannot be parsed as CSV at all, a specific line has more fields than the header defines"),
        gold_classification="schema", gold_action="escalate",
        notes="A malformed/corrupt file needs a human to look at the source export, not an automated schema change.",
    ),
    EvalCase(
        id="schema-type-mismatch-in-filter",
        error_type="TransformationStepError",
        error_message="Type mismatch: Cannot cast filter value '2026-09-17' to int64 for column 'store_id'",
        log_analysis=la("transformation", "failed on first attempt, no retries configured for transformation",
                         "a column that the pipeline config expects to be numeric now contains a value that doesn't cast to that type"),
        gold_classification="schema", gold_action="escalate",
        acceptable_alternate_action="schema_evolution",
        notes="A column's type appears to have drifted (store_id used to be numeric-compatible, now isn't). Plausible for schema_evolution in principle, but the safer default when a type (not just a missing column) has changed is escalate, since blindly evolving a type conversion risks silently corrupting downstream data. Scores partial credit for schema_evolution.",
    ),
    EvalCase(
        id="schema-load-write-rejected-by-warehouse",
        error_type="LoadStepError",
        error_message="Failed to write table to data warehouse: column \"discount_pct\" is of type integer but expression is of type numeric",
        log_analysis=la("load", "failed on first attempt, no retries configured for load",
                         "the data warehouse rejected the write because an existing table column's type conflicts with the incoming data's type"),
        gold_classification="schema", gold_action="schema_evolution",
        notes="This is the cleanest schema_evolution case at the load step. The existing warehouse table's column type needs to widen (integer to numeric), exactly the kind of schema change that action describes.",
    ),

    # ---- config bugs: don't fit any category well (the taxonomy gap) ----
    EvalCase(
        id="config-missing-source-url",
        error_type="IngestionStepError",
        error_message="No source_url parameter. Ingestion step requires a source_url in config",
        log_analysis=la("ingestion", "failed on first attempt, no retries configured",
                         "the ingestion step's own configuration is missing a required parameter, this is not a runtime data or network failure, the step was never going to succeed"),
        gold_classification="unknown", gold_action="escalate",
        notes="Included as a taxonomy-gap case: this is a pipeline definition bug, not a data failure. None of the five categories describe it well; 'unknown' plus escalate is the least-wrong answer, and the finding itself belongs in the eval report, not just the label.",
    ),
    EvalCase(
        id="config-transformation-unknown-filter-column",
        error_type="TransformationStepError",
        error_message="Unknown filter column: 'regoin'",
        log_analysis=la("transformation", "failed on first attempt, no retries configured for transformation",
                         "the pipeline's filter configuration references a column name that doesn't exist in the data, looks like a typo in the pipeline config itself ('regoin')"),
        gold_classification="unknown", gold_action="escalate",
        acceptable_alternate_classification="schema",
        notes="A typo'd column name in the pipeline's own config, not the source data. schema is defensible (scored partial-credit) but the root cause is a config bug, same taxonomy gap as the source_url case.",
    ),
    EvalCase(
        id="config-orchestration-validation-before-ingestion",
        error_type="ValidationStepError",
        error_message="Validation step requires ingestion output file_path in run_context",
        log_analysis=la("validation", "no attempt pattern available, this step appears to have run out of order",
                         "the validation step ran without ingestion having produced its output first, which should not be possible under normal pipeline execution",
                         notable_signals=["step ran out of expected order"]),
        gold_classification="unknown", gold_action="escalate",
        notes="Signals an executor/orchestration bug, not a data problem; the pipeline's own step sequencing broke. Nothing about retrying or evolving a schema addresses this.",
    ),

    # ---- partial_load: beyond current step_handlers.py granularity, see module docstring ----
    EvalCase(
        id="partial-load-constraint-violation-midbatch",
        error_type="LoadStepError",
        error_message="Failed to write table to data warehouse: 8,412 of 15,000 rows inserted before a UNIQUE constraint violation on order_id=88213 aborted the write",
        log_analysis=la("load", "failed on first attempt, no retries configured for load",
                         "the write to the data warehouse was interrupted partway through, a majority of rows were written before a duplicate-key error stopped it"),
        gold_classification="partial_load", gold_action="replay_from_raw",
        notes="Synthetic beyond current granularity (see module docstring): run_load's except clause can't currently produce a rows-written count. Included to test whether the model correctly identifies partial_load given that signal; making step_handlers.py surface it is a prerequisite follow-up.",
    ),
    EvalCase(
        id="partial-load-connection-dropped-midwrite",
        error_type="LoadStepError",
        error_message="Failed to write table to data warehouse: connection to database lost after approximately 60% of rows were written",
        log_analysis=la("load", "failed on first attempt, no retries configured for load",
                         "the database connection dropped mid-write, after a majority but not all of the rows had been inserted"),
        gold_classification="partial_load", gold_action="replay_from_raw",
        notes="Same synthetic caveat as above. Tests recognition of 'some rows landed, then it broke' framing, distinct from a clean network failure during load.",
    ),
    EvalCase(
        id="partial-load-vs-clean-network-failure-at-load-step",
        error_type="LoadStepError",
        error_message="Failed to write table to data warehouse: could not connect to server: Connection refused",
        log_analysis=la("load", "failed on first attempt, no retries configured for load",
                         "the load step could not even establish a connection to the data warehouse, no indication any rows were written"),
        gold_classification="network", gold_action="retry_with_backoff",
        notes="Paired with the two partial_load cases above: same step (load), but a connection refusal with zero evidence of a partial write is plainly network, not partial_load. Tests that the model doesn't over-apply partial_load to every load-step failure just because it's the load step.",
    ),

    # ---- unknown: genuinely ambiguous / generic ----
    EvalCase(
        id="unknown-bare-keyerror",
        error_type="KeyError",
        error_message="'tenant_id'",
        log_analysis=la("unknown", "cannot determine, the error is a bare KeyError with no descriptive message",
                         "an unexpected internal error occurred; a dictionary key was missing somewhere in the pipeline's own code, not in the source data",
                         notable_signals=["error message has no descriptive content"]),
        gold_classification="unknown", gold_action="escalate",
        notes="A bare internal exception with no message content. There's nothing here to classify into a substantive category, and the model should say so rather than guess.",
    ),
    EvalCase(
        id="unknown-generic-runtime-error",
        error_type="RuntimeError",
        error_message="unexpected state",
        log_analysis=la("transformation", "cannot determine attempt pattern, no descriptive error content",
                         "a generic runtime error with a non-specific message, giving no clue about network, schema, or quota involvement"),
        gold_classification="unknown", gold_action="escalate",
        notes="Same shape as above at a different step. Confirms the pattern isn't step-specific.",
    ),
    EvalCase(
        id="unknown-conflicting-signals",
        error_type="IngestionStepError",
        error_message="Failed to fetch from https://api.vendor.com/export: 429 Too Many Requests, then retry succeeded, then validation failed with a schema error downstream in the same run",
        log_analysis=la("validation", "ingestion initially hit a 429 and succeeded on retry; the actual failure that ended the run happened later, in validation",
                         "the proximate failure is a schema issue, but the run_context also shows an unrelated quota event earlier in the same run that resolved on its own"),
        gold_classification="schema", gold_action="escalate",
        notes="Tests whether the model classifies based on the failure that ended the run (a downstream schema error) rather than anchoring on an earlier, already-resolved quota blip mentioned in the same context.",
    ),

    # ---- degraded upstream (log_analysis_failed sentinel) -> should lean unknown/escalate regardless of raw error ----
    EvalCase(
        id="degraded-log-analysis-on-what-would-be-clean-network-error",
        error_type="ConnectTimeout",
        error_message="connection timed out after 30s",
        log_analysis=la("unknown", "Could not analyze step attempts due to log analysis failure.",
                         "Raw error passed through without interpretation: ConnectTimeout - connection timed out after 30s",
                         notable_signals=["log_analysis_failed"]),
        gold_classification="unknown", gold_action="escalate",
        notes="Even though the raw error_type/message look like an easy network case, the log_analysis_failed signal means the upstream pipeline is degraded. Per the prompts' own instruction, both nodes should reflect that with lower confidence and lean toward escalate rather than guessing network/retry_with_backoff from the raw string alone.",
    ),
    EvalCase(
        id="degraded-log-analysis-on-what-would-be-clean-quota-error",
        error_type="IngestionStepError",
        error_message="Failed to fetch from https://api.vendor.com/data: 429 Too Many Requests",
        log_analysis=la("unknown", "Could not analyze step attempts due to log analysis failure.",
                         "Raw error passed through without interpretation: IngestionStepError - Failed to fetch from https://api.vendor.com/data: 429 Too Many Requests",
                         notable_signals=["log_analysis_failed"]),
        gold_classification="unknown", gold_action="escalate",
        notes="Same degraded-input test as above with a more 'obvious' raw error (429). The more tempting the raw string is to pattern-match on, the better this tests whether the model respects the degradation signal instead of ignoring it.",
    ),
    EvalCase(
        id="degraded-log-analysis-with-low-confidence-classification-should-still-escalate",
        error_type="TransformationStepError",
        error_message="Unable to apply filter store_id > 100: unexpected error",
        log_analysis=la("unknown", "Could not analyze step attempts due to log analysis failure.",
                         "Raw error passed through without interpretation: TransformationStepError - Unable to apply filter store_id > 100: unexpected error",
                         notable_signals=["log_analysis_failed"]),
        gold_classification="unknown", gold_action="escalate",
        notes="Third degraded case, at a third step, to make sure the pattern holds generally and isn't an artifact of the specific error type used in the first two.",
    ),

    # ---- retrieval ablation set: these carry a hand-authored runbook/incident chunk that argues for a
    # non-default action. run_eval.py runs the whole suite twice (with context, then with it stripped to
    # []). These are the cases where dropping the context should plausibly change the predicted action,
    # which is what makes them useful as an ablation rather than just more accuracy cases.
    EvalCase(
        id="retrieval-runbook-overrides-default-network-action",
        error_type="IngestionStepError",
        error_message="Failed to fetch from https://api.legacyvendor.com/feed: 500 Internal Server Error",
        log_analysis=la("ingestion", "failed on first attempt with no retries configured",
                         "the external source returned a 500, generically indicating a server-side problem"),
        retrieved_context=[
            chunk("runbook", "network.md#legacyvendor-quirks",
                  "api.legacyvendor.com is known to return HTTP 500 for a few minutes during its nightly maintenance window (02:00-02:15 UTC). "
                  "Retrying immediately during this window has caused duplicate downstream records in the past. "
                  "For failures against this specific host, prefer pause_schedule over retry_with_backoff and let the next scheduled run pick it up after the window closes.",
                  0.91),
        ],
        gold_classification="network", gold_action="pause_schedule",
        expected_action_without_context="retry_with_backoff",
        notes="Without the runbook, a bare 500 is the textbook retry_with_backoff case (see net-5xx-upstream above); this case is identical in error shape, but the retrieved runbook chunk overrides that default for this one host. With context stripped in the ablation run, a correct model should fall back to retry_with_backoff, which is the expected answer for that run, not a failure.",
    ),
    EvalCase(
        id="retrieval-past-incident-suggests-escalate-over-retry",
        error_type="IngestionStepError",
        error_message="Failed to fetch from https://api.partner.com/orders: connection timed out after 30s",
        log_analysis=la("ingestion", "failed on first attempt with no retries configured",
                         "a connection timeout while fetching from an external HTTP source"),
        retrieved_context=[
            chunk("past_recommendation", "rec_id=214",
                  "run_id=1988, same pipeline, same host: classified as network, recommended retry_with_backoff. Retried automatically 4 times over 2 hours, "
                  "all 4 attempts timed out identically, and the vendor's status page later confirmed a multi-day outage. Operator dismissed the recommendation and manually paused the pipeline instead.",
                  0.88),
        ],
        gold_classification="network", gold_action="pause_schedule",
        expected_action_without_context="retry_with_backoff",
        notes="A single timeout looks identical to net-connect-timeout-first-attempt above, but here a past incident on the SAME pipeline/host shows retry_with_backoff already failed repeatedly against what turned out to be a multi-day outage. Grounded recovery planning should weigh that history over the default. With context stripped, retry_with_backoff is the expected fallback answer, not a wrong one.",
    ),
    EvalCase(
        id="retrieval-runbook-confirms-default-no-override",
        error_type="IngestionStepError",
        error_message="Failed to fetch from https://api.ratelimited-vendor.com/data: 429 Too Many Requests",
        log_analysis=la("ingestion", "failed on first attempt with no retries configured",
                         "the external API rejected the request due to rate limiting"),
        retrieved_context=[
            chunk("runbook", "quota.md#rate-limits",
                  "For any 429 response, retry with exponential backoff starting at 30s. This resolves the large majority of rate-limit failures across all integrations.",
                  0.94),
        ],
        gold_classification="quota", gold_action="retry_with_backoff",
        expected_action_without_context="retry_with_backoff",
        notes="Control case for the ablation set: the runbook here CONFIRMS the default action rather than overriding it, so this case's predicted action should be identical with or without context. Included so the ablation isn't only measuring 'does retrieval change the answer' but also 'does retrieval ever break a case that was already correct.'",
    ),
]


CASES_BY_ID = {c.id: c for c in CASES}

RETRIEVAL_ABLATION_CASE_IDS = [
    "retrieval-runbook-overrides-default-network-action",
    "retrieval-past-incident-suggests-escalate-over-retry",
    "retrieval-runbook-confirms-default-no-override",
]
