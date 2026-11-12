# Reliability

The platform promises that a run is not lost and a schedule is kept. This page shows where that was not true, what was changed, how it was checked, and what is still not guaranteed.

## Four problems, found by making them happen

Each one was reproduced with the real API and worker as separate processes, a real Postgres and Redis, and a real fault: stopping the Redis container, killing the worker process, waiting out a timer. The same script was run before and after the changes.

| | Fault | Before | After |
|---|---|---|---|
| E1 | Redis is down when a run is created | The API answered `500`. The run was saved as `queued` and nothing would ever start it. A retry with the same `Idempotency-Key` answered `200` and did not queue it again. 30 s later: `queued`. | The API answers `201`, because the run exists. The reaper put it on the queue once Redis was back. Finished as `success` within 90 s. |
| E2 | The worker is killed in the middle of a run | The run stayed `running` for ever. A new worker did nothing with it. 50 s later: `running`. | The lease ran out, the reaper put the run back, a second worker finished it. `success`, `attempts = 2`, 66 s after the kill. |
| E3 | A circuit breaker opens, the cause is fixed, the wait time passes | The breaker stayed `open` and every run was `blocked`, until a person set it to `half-open` through the API. | The next run after `retry_after` turned the breaker `half-open`, ran as the trial, succeeded and closed the breaker. |
| E4 | A schedule with `* * * * *` | The API accepted it and nothing read it. After 130 s: 0 runs. | After 130 s: 2 runs, one per minute, each `success`. |

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

### A circuit breaker that recovers (E3)

When a run arrives and its pipeline's breaker is `open` with `retry_after` in the past, one conditional `UPDATE` moves it to `half-open` and the run goes ahead as the trial. When several workers get there together exactly one makes the change and the others see `half-open` and go ahead. A success closes the breaker, a failure opens it again for another full wait. A breaker opened by hand with no `retry_after` stays open until a person changes it.

While a breaker is `half-open` every run is let through, not only the first. That was already how the half-open state behaved when a person set it, and it was kept. A stricter version (exactly one trial in flight) needs a second state or a lock, and is listed below.

### A schedule that fires (E4)

`worker/app/scheduler.py` runs in every worker. There is no leader: running it twice at once is safe.

- The run for one cron time is created with the idempotency key `schedule:<schedule id>:<fire time in UTC>`. The existing unique constraint on `(pipeline_id, idempotency_key)` means exactly one run is created however many workers try. Only the worker whose insert won queues it.
- `schedules.handled_until` says that every fire time up to then is dealt with. A schedule is due when its newest fire time that is not in the future is after that mark.
- Catch-up policy: if several fire times were missed (all workers were down for an hour of an every-minute schedule) one run is started for the newest and the rest are passed over. A new schedule, a changed cron expression or time zone, and a schedule switched back on all start from their next fire time; they never run for the time they did not exist or were paused.
- Fire times are worked out with `croniter` in the schedule's time zone, and the clock is the database's `now()`.
- A worker that stops after creating the run and before recording the mark is harmless: the next check works out the same fire time, finds the run that exists and records it. If it also never queued the run, the reaper does.

## How it was checked

- **Unit tests** with fakes for the executor and for the cron arithmetic. These run on every push.
- **Integration tests** (`tests/integration/`, 32 of them) against a real Postgres and a real Redis, run in CI by the `Integration tests` job. They include: the queue down and then back; an id dropped from the queue; a dead worker taken over; giving up after the attempt limit; a live lease left alone; the same id pushed 20 times running once; 40 runs on 6 concurrent workers each running exactly once; 5 reapers putting each run on the queue once; a breaker recovering after real failures; 8 runs arriving together when a wait ends; 10 workers checking one schedule at once creating one run; and a real `python -m worker.app.main` process killed in the middle of a run with a second process finishing it, and one that starts a scheduled run.
- **Mutation checks.** Each important line was broken on purpose (no idempotency key, no lease extension, the lost-the-race branch, the clock-time comparison and so on) and the tests had to fail. `scripts/mutation_check_executor.py` does this for the executor (17 changes, all caught). The reaper, lease and scheduler were checked the same way once, by hand. Two changes were found that no test could notice, and both are redundant on purpose: a Python check that repeats a condition already in the SQL `WHERE`, and a reload of the breaker after the `UPDATE`.
- **Repeated runs.** The in-process integration tests for the queue and the lease were run six times in a row with no failure.

## Throughput

60 runs were queued first, then 1, 2, 4 and 8 real worker processes drained them. Each run reads a source that answers after 1 s. Time is from the first run starting to the last one ending, from the database. All on one laptop with Postgres and Redis in Docker.

| Workers | Time for 60 runs | Runs per second |
|---|---|---|
| 1 | 90.8 s | 0.66 |
| 2 | 51.8 s | 1.16 |
| 4 | 28.0 s | 2.14 |
| 8 | 22.7 s | 2.64 |

Every run finished once (`attempts = 1`) in every round. One worker takes about 1.5 s per run: 1 s waiting for the source and the rest in the database, the file write and the diagnostic hand-off. Four workers are 3.2 times faster than one. Going to eight gains little because all the processes, the database and Redis share one machine. This says that workers add throughput and never run a run twice. It does not say what the platform does on real hardware.

Creating runs through the API took about 0.5 s per request when a single client sent them one after another, so a test that creates runs through the API measures the client, not the workers.

## What is still not guaranteed

- **At least once, not exactly once.** A taken-over run executes its steps again. Steps are repeatable, but a webhook for the first attempt may already have gone out, and a step with an outside effect would repeat it.
- **No fencing for a slow worker.** If a worker is only slow, not dead (a long pause, a network split), its lease can run out and a second worker takes the run while the first is still going. The first worker's later writes are not rejected. The window is `RUN_LEASE_SECONDS`, 60 by default, and the heartbeat is there to prevent it; closing it fully needs a fencing token on every write.
- **A run is lost only if Postgres is lost.** Redis holds only ids that can be rebuilt from Postgres. Losing Redis loses nothing but time.
- **Schedules can overlap.** A run that lasts longer than the gap between cron times overlaps with the next one. Nothing stops that.
- **A cron time is met within `SCHEDULER_INTERVAL_SECONDS`** (10 by default), and only to the minute for five-field expressions.
- **The half-open state lets every run through** until the first one finishes, as described above.
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
- `circuit_breaker_transitions_total{to_state}`: now includes `half-open`.
