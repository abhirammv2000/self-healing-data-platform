# Reliability

The platform promises that a run is not lost and a schedule is kept. This page shows where that was not true, what was changed, how it was checked, and what is still not guaranteed.

## Six problems, found by making them happen

E1 to E4 were reproduced with the real API and worker as separate processes, a real Postgres and Redis, and a real fault: stopping the Redis container, killing the worker process, waiting out a timer. The same script was run before and after the changes. E5 was reproduced in a test process against the real Postgres with a worker made to stall. E6 could **not** be reproduced as a failure, and is a rule added on purpose, as its row says.

| | Fault | Before | After |
|---|---|---|---|
| E1 | Redis is down when a run is created | The API answered `500`. The run was saved as `queued` and nothing would ever start it. A retry with the same `Idempotency-Key` answered `200` and did not queue it again. 30 s later: `queued`. | The API answers `201`, because the run exists. The reaper put it on the queue once Redis was back. Finished as `success` within 90 s. |
| E2 | The worker is killed in the middle of a run | The run stayed `running` for ever. A new worker did nothing with it. 50 s later: `running`. | The lease ran out, the reaper put the run back, a second worker finished it. `success`, `attempts = 2`, 66 s after the kill. |
| E3 | A circuit breaker opens, the cause is fixed, the wait time passes | The breaker stayed `open` and every run was `blocked`, until a person set it to `half-open` through the API. | The next run after `retry_after` turned the breaker `half-open`, ran as the trial, succeeded and closed the breaker. |
| E4 | A schedule with `* * * * *` | The API accepted it and nothing read it. After 130 s: 0 runs. | After 130 s: 2 runs, one per minute, each `success`. |
| E5 | A worker is paused, not dead: its lease runs out, a second worker finishes the run, then the first wakes up and fails | The second worker's `success` was replaced by the first worker's late failure. The run ended `failed` with `attempts = 2`, and the failure counted towards the circuit breaker. | The late failure changes nothing. The run stays `success`, `attempts = 2`, no error recorded, the breaker is untouched. Covered by an integration test, not re-run as separate processes. |
| E6 | Several runs of one pipeline at the same time | Allowed. 48 runs of one pipeline on 6 real workers all succeeded, so **no collision on the warehouse table was reproduced**. Nothing stopped one, and a schedule slower than its interval overlapped with itself. | At most one run of a pipeline is active. 48 runs on 6 workers never had two active at once, all `success` with `attempts = 1`, in 25.2 s. This is a policy, not a bug fix: see below. |

The script is not in the repository (it is a harness for one machine). The same faults are covered by tests that are: `tests/integration/` (see below).

## How each one works

### A run that never reached the queue (E1)

A run is saved in Postgres first and put on the Redis queue second. Those are two systems, so the second can fail after the first worked. Making both atomic is not possible across Postgres and Redis, so the design makes the gap harmless:

- The run row is the source of truth. `shared/run_queue.py` `enqueue_run()` pushes the id and then records `last_enqueued_at`. If Redis fails it logs, returns `False` and does not raise, so the API answers `201`: the run exists and will start.
- The reaper (`worker/app/reaper.py`) finds `queued` runs with no `last_enqueued_at`, or one older than `ENQUEUE_VISIBILITY_SECONDS`, and pushes them again. That also covers an id that a worker popped and then died holding.
- Pushing an id twice is harmless. A worker claims a run with one conditional `UPDATE ... WHERE status = 'queued'`, and only one can win. The others see the run already taken and move on.

This is the same idea as a transactional outbox, with the run table acting as the outbox and the reaper as the relay.

### A worker that dies (E2)

A claimed run has a lease: `lease_expires_at`, set when the run is claimed. The worker extends it every third of `RUN_LEASE_SECONDS` while it works (`worker/app/lease.py`). The reaper takes any `running` run whose lease is in the past, puts it back to `queued` and queues it, or fails it with `error_type = worker_lost` after `RUN_MAX_ATTEMPTS` claims, so a run that kills its worker every time does not loop for ever.

Lease times use the database's `now()` and `timestamptz` columns, never a worker's clock, so workers on different machines need no synchronised clocks. Reapers lock the rows they take with `FOR UPDATE SKIP LOCKED`, so many workers can run one without taking the same run.

A taken-over run starts again from its first step. That is safe because the steps can be repeated: ingestion writes a new file and the load step replaces its table (`if_exists="replace"`). A new kind of step has to keep that true.

### A worker that is slow, not dead (E5)

A lease cannot tell a dead worker from a paused one. So every claim of a run gets a number, `attempts`, which acts as a fencing token (the idea from Martin Kleppmann's note on distributed locks): the lease says who may work, the token lets the database refuse a worker that is no longer allowed.

- `_claim_run` returns the new `attempts` value (`UPDATE ... RETURNING`), and the worker keeps it as its token.
- Before the worker writes to the run (a retry, the success, the failure) it runs `_fence`: `SELECT attempts, status FROM pipeline_runs WHERE id = ... FOR UPDATE`. If the token differs or the run is no longer running, it raises `LeaseLostError`, releases the lock, and writes nothing: no failure, no breaker change, no agent, no webhook. The row lock stays until the write commits, so the reaper cannot hand the run over between the check and the write.
- The heartbeat carries the token too. A worker that lost the run can no longer extend the new owner's lease, which it could before. When the heartbeat finds the run gone it sets `lost`, and the executor stops before its next step instead of running the rest of a run it no longer owns.

What this does not cover: a step that is already running when the lease is lost keeps going until it ends, and its outside effects (a file, the warehouse table, a request) are not fenced. See the limits.

### One run of a pipeline at a time (E6)

Postgres has a partial unique index, `uq_pipeline_runs_one_active`, on `pipeline_runs (pipeline_id) WHERE status IN ('running', 'retrying')`. Claiming a second run of a pipeline that has one active fails in the database, whichever worker tries, and the slot is free again when the run ends or the reaper takes it back from a dead worker. The index is the lock. Compared with a Redis or advisory lock it has no timeout to tune, cannot be lost with Redis, and needs no connection held for the length of a run.

- A worker that cannot claim a run for this reason leaves it `queued` (`pipeline_run_deferred`, `pipeline_runs_deferred_total`), marks it as just offered, and goes on to the next id. It does not use an attempt and does not wait.
- When a run ends, its worker puts the oldest waiting run of that pipeline on the queue at once (`wake_waiting_run`), before the slow part of finishing. If that fails, the reaper offers the waiting run after `ENQUEUE_VISIBILITY_SECONDS`.
- The migration moves any extra active runs of a pipeline back to `queued` (the newest one is left), because a database from before this can hold several and the index could not be built otherwise. It was tested with seeded duplicates.
- Runs of different pipelines are not held back by each other (tested, and the throughput below).

Why it was added when no failure could be shown: the warehouse table is named per tenant and pipeline and every run replaces it, so two overlapping runs mean the last one to finish decides which data is there, not the newest data. Running one at a time also removes the overlapping-schedule gap and makes the half-open breaker run one trial at a time in practice. It is also a change in behaviour: a burst of runs for one pipeline now takes turns.

### A circuit breaker that recovers (E3)

When a run arrives and its pipeline's breaker is `open` with `retry_after` in the past, one conditional `UPDATE` moves it to `half-open` and the run goes ahead as the trial. When several workers get there together exactly one makes the change and the others see `half-open` and go ahead. A success closes the breaker, a failure opens it again for another full wait. A breaker opened by hand with no `retry_after` stays open until a person changes it.

While a breaker is `half-open` every run passes the breaker check, as it did when a person set that state, but because a pipeline now runs one run at a time (E6) the others wait, and they check the breaker again when their turn comes. So the first run decides: if it fails and reopens the breaker, the waiting ones are blocked, and if it succeeds they run. A run that passed the check just before the trial failed can still run.

### A schedule that fires (E4)

`worker/app/scheduler.py` runs in every worker. There is no leader: running it twice at once is safe.

- The run for one cron time is created with the idempotency key `schedule:<schedule id>:<fire time in UTC>`. The existing unique constraint on `(pipeline_id, idempotency_key)` means exactly one run is created however many workers try. Only the worker whose insert won queues it.
- `schedules.handled_until` says that every fire time up to then is dealt with. A schedule is due when its newest fire time that is not in the future is after that mark.
- Catch-up policy: if several fire times were missed (all workers were down for an hour of an every-minute schedule) one run is started for the newest and the rest are passed over. A new schedule, a changed cron expression or time zone, and a schedule switched back on all start from their next fire time; they never run for the time they did not exist or were paused.
- Fire times are worked out with `croniter` in the schedule's time zone, and the clock is the database's `now()`.
- A worker that stops after creating the run and before recording the mark is harmless: the next check works out the same fire time, finds the run that exists and records it. If it also never queued the run, the reaper does.

## How it was checked

- **Unit tests** with fakes for the executor and for the cron arithmetic. These run on every push.
- **Integration tests** (`tests/integration/`, 47 of them) against a real Postgres and a real Redis, run in CI by the `Integration tests` job. They include: the queue down and then back; an id dropped from the queue; a dead worker taken over; giving up after the attempt limit; a live lease left alone; the same id pushed 20 times running once; 40 runs on 6 concurrent workers each running exactly once; 5 reapers putting each run on the queue once; a breaker recovering after real failures; 8 runs arriving together when a wait ends; 10 workers checking one schedule at once creating one run; a paused worker that wakes up and fails not overwriting the worker that finished the run; 10 runs of one pipeline on 6 workers never active at once and all finishing without the reaper; and a real `python -m worker.app.main` process killed in the middle of a run with a second process finishing it, and one that starts a scheduled run.
- **Mutation checks.** Each important line was broken on purpose (no idempotency key, no lease extension, the lost-the-race branch, the clock-time comparison and so on) and the tests had to fail. `scripts/mutation_check_executor.py` does this for the executor (29 changes, all caught). The reaper, lease, scheduler, fencing, one-run-per-pipeline and wake-up code were checked the same way by hand, including dropping or weakening the database index. Some changes survived at first, which showed tests that were too weak (a test whose data could not tell the right query from the wrong one, a missing test for a failing database read); those tests were strengthened until the changes were caught. Two changes cannot be noticed by any test and are redundant on purpose: a Python check that repeats a condition already in the SQL `WHERE`, and a reload of the breaker after the `UPDATE`. Writing the fencing change also caught a real mistake of mine in review: a loop variable called `attempt` hid the fencing token, and a unit test failed on it.
- **Repeated runs.** The in-process integration tests for the queue and the lease were run six times in a row with no failure.

## Throughput

60 runs, 5 for each of 12 pipelines, were queued first, then 1, 2, 4 and 8 real worker processes drained them. Each run reads a source that answers after 1 s. Time is from the first run starting to the last one ending, from the database. All on one laptop with Postgres and Redis in Docker.

| Workers | Time for 60 runs | Runs per second |
|---|---|---|
| 1 | 87.3 s | 0.69 |
| 2 | 47.4 s | 1.26 |
| 4 | 28.3 s | 2.12 |
| 8 | 22.1 s | 2.72 |

Every run finished once (`attempts = 1`) in every round. One worker takes about 1.4 s per run: 1 s waiting for the source and the rest in the database, the file write and the diagnostic hand-off. Four workers are 3.1 times faster than one. (The first version of this table put all 60 runs on one pipeline. Since a pipeline now runs one run at a time that would no longer scale with workers, so the runs were spread over 12 pipelines. The old figures were 0.66, 1.16, 2.14 and 2.64 runs per second.)

A single pipeline is serial by design: 48 queued runs of one four-step pipeline took 25.2 s on 6 workers, about 0.5 s each, with never more than one active. Going to eight gains little because all the processes, the database and Redis share one machine. This says that workers add throughput and never run a run twice. It does not say what the platform does on real hardware.

Creating runs through the API took about 0.5 s per request when a single client sent them one after another, so a test that creates runs through the API measures the client, not the workers.

## What is still not guaranteed

- **At least once, not exactly once.** A taken-over run executes its steps again. Steps are repeatable, but a webhook for the first attempt may already have gone out, and a step with an outside effect would repeat it.
- **Fencing covers the run's record, not what the steps do.** A worker that lost its lease cannot change the run, the breaker or send the webhook, and stops before its next step. But a step already in progress runs to its end, and its outside effects are not fenced: the file it writes, the warehouse table it replaces, a request it sends. Two workers can briefly work on the same run, and the warehouse table could be written by the old worker after the new one. Fencing those needs the token passed into each step and checked by the thing being written.
- **A run is lost only if Postgres is lost.** Redis holds only ids that can be rebuilt from Postgres. Losing Redis loses nothing but time.
- **Scheduled runs can pile up.** Runs of a pipeline no longer overlap, but if a run takes longer than the gap between cron times the next ones are created anyway and wait, so a backlog grows. A "skip if the last one is still going" option would stop that.
- **One pipeline, one run at a time.** A burst of runs for one pipeline goes through serially. A waiting run is normally started the moment the one before it ends, and up to `ENQUEUE_VISIBILITY_SECONDS` later if that wake-up is lost.
- **A cron time is met within `SCHEDULER_INTERVAL_SECONDS`** (10 by default), and only to the minute for five-field expressions.
- **The scheduler reads every active schedule each interval.** That is fine for thousands of schedules and not for millions.
- **Recovery takes time.** A dead worker's run waits for its lease to run out (60 s by default) plus up to one reaper interval. A shorter lease means faster recovery and more risk of taking over a slow worker's run.
- **Not run on AWS.** All of this was tested on a laptop. The EKS deployment in `infra/` was built and torn down before these changes and has not been redeployed with them.

## Settings

| Variable | Default | What it does |
|---|---|---|
| `RUN_LEASE_SECONDS` | 60 | How long a claimed run belongs to its worker without a sign of life. The worker extends it every third of this. |
| `REAPER_INTERVAL_SECONDS` | 10 | How often each worker looks for lost runs. |
| `ENQUEUE_VISIBILITY_SECONDS` | 30 | A queued run last put on the queue longer ago than this, and not claimed, is put there again. |
| `RUN_MAX_ATTEMPTS` | 3 | A run whose worker stops this many times is failed with `worker_lost`. |
| `SCHEDULER_INTERVAL_SECONDS` | 10 | How often each worker checks for due schedules. |
| `WORKER_METRICS_PORT` | 8001 | The port a worker serves `/metrics` on. Two workers on one machine need different ports. |

A wrong value stops the process at startup and names the variable.

## Metrics

- `pipeline_runs_recovered_total{reason}`: `queued_again`, `requeued_worker_lost`, `failed_worker_lost`. Every count is a run that would have been stuck.
- `scheduled_runs_total`: runs started by a cron time.
- `pipeline_runs_deferred_total`: times a worker left a run queued because its pipeline already had a run going.
- `circuit_breaker_transitions_total{to_state}`: now includes `half-open`.
