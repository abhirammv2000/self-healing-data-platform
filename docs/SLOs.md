# Service level objectives

Three objectives, each with alerts that page or open a ticket before the error budget is gone. The rules live in `observability/rules/`, the dashboard is "Self-healing data platform: SLOs" in Grafana, and every alert links to a runbook in `docs/runbooks/`.

## The objectives

| Objective | Indicator | Target over 30 days | Error budget |
|---|---|---|---|
| API availability | Control plane requests that do not return 5xx | 99.5% | 0.5% of requests |
| API latency | Control plane requests answered in under 0.5s | 95% | 5% of requests |
| Diagnosis coverage | Failed pipeline runs that get a recommendation | 95% | 5% of failed runs |

Prometheus's own scrapes of `/metrics` are left out of the HTTP indicators. They arrive every few seconds and always succeed, so counting them would make the service look healthier than it is.

The latency indicator uses the 0.5s bucket of `http_request_duration_seconds`, which is the largest bucket at or below the target in the instrumentator's default buckets (0.1s, 0.5s, 1s). That is why the target is 0.5s and not something rounder.

A recommendation counts for diagnosis coverage even when the agent was degraded and fell back to "escalate", because the operator still gets something to act on. Coverage is about whether a failed run is left with nothing.

## What is not an objective

**Pipeline run success rate.** A run fails whenever a customer's data source is down or sends bad data, and the platform can't prevent that. Holding the platform to a success target would mean paging someone for a problem they can't fix. It has an alert (more than half of recent runs failing) that opens a ticket instead, and a panel on the dashboard.

**Run duration.** `pipeline_run_duration_seconds` uses the client library's default buckets, which stop at 10 seconds. A run that takes longer lands in the last bucket and can't be measured. Adding real buckets is a change to `shared/metrics.py`, not something a rule can fix.

## How alerts work

Alerts use multiple windows and multiple burn rates, the approach from the Google SRE workbook. A burn rate of 1 uses exactly the 30 day budget in 30 days. An alert fires only when both a long and a short window are above its threshold. The long window keeps it from firing on a blip, and the short window makes it stop soon after the problem is fixed.

| Burn rate | Windows | Severity | Meaning |
|---|---|---|---|
| 14.4x | 1h and 5m | page | 2% of the budget gone in an hour |
| 6x | 6h and 30m | page | 5% of the budget gone in six hours |
| 3x | 1d and 2h | ticket | 10% gone in a day |
| 1x | 3d and 6h | ticket | 10% gone in three days |

These apply to availability and latency. Diagnosis coverage has one ticket alert instead, which also needs at least 5 failed runs in the window, because it has too little traffic for burn rates to mean much.

Some things a ratio can't see, like a process that has stopped and so sends nothing. Those have their own alerts: `ControlPlaneDown`, `WorkerDown`, `PipelineFailureRateHigh` and `CircuitBreakersOpening`. `alertmanager.yml` also stops a page from being followed by a ticket for the same objective, and mutes the availability burn alerts while the control plane is down.

## Error budget policy

If the availability or latency budget is spent, stop feature work on the control plane until it is back under budget, and look at what burned it first. This is a one-person project, so the policy is short on purpose. The point of writing it down is that the choice has been made in advance.

## Trying it

```bash
# validate and unit test the rules, no running stack needed
promtool check rules observability/rules/slo.rules.yml observability/rules/slo.alerts.yml
promtool test rules observability/rules/tests/slo.test.yml

# run the stack (Prometheus :9090, Alertmanager :9093, Grafana :3001)
docker compose up -d prometheus alertmanager grafana
python scripts/alert_receiver.py     # prints alerts as Alertmanager sends them
```

The tests feed synthetic series through the real rule files and check what fires: a steady 10% error rate pages, 0.2% never alerts, a burn rate of 8 pages the slow alert but not the fast one, and the fast alert stops once the short window is clean again.

## Limits

- The thresholds have not been tuned against real traffic. Everything above was checked with synthetic data: the unit tests, and one live run of the real Prometheus and Alertmanager against a stand-in service returning 50% errors.
- Prometheus keeps 35 days here so the 30 day windows have something to look at. The "30 day" panels show whatever data exists so far until then.
- The p95 latency panel is coarse. With buckets at 0.1s, 0.5s and 1s it can only say which bucket the 95th percentile falls in.
- Alerts go to a local webhook receiver. A real deployment would point Alertmanager at a paging service and a ticket queue.
