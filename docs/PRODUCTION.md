# Running it in production

What was changed to make the platform safe to run for other people, how each change was checked, and what is still not done. The run-recovery work (leases, the reaper, fencing, one run per pipeline) is in [RELIABILITY.md](RELIABILITY.md). This page is about the edges: configuration, health checks, shutdown, the images, the chart, and what a tenant is allowed to make the worker call.

## What changed

| # | Problem found | What it did | What it does now |
|---|---|---|---|
| P1 | A missing `ADMIN_SECRET_KEY` or `API_KEY_SECRET` | The process started. Every request that needed the secret answered 500. A blank secret was accepted. | Both programs stop at startup and name every missing setting. With `ENV=production` the two secrets must be 16 characters or longer and different from each other. |
| P2 | `/health` was the only check, and the chart used it for readiness | A pod with no database still got traffic. | `/health` (alive, looks at nothing else) and `/ready` (Postgres and Redis both answer, 2 seconds each, 503 otherwise). The chart uses `/ready` for readiness and `/health` for liveness, so a database outage removes pods from the Service but does not restart them. |
| P3 | A worker was stopped with SIGTERM (every deploy, scale-down and node drain) | It died on the spot. The run in hand sat in `running` until its lease ran out, about a minute, and was then done again from step one. | On SIGTERM the worker finishes the run it has, takes no new one, stops the reaper and scheduler, and exits with code 0. The chart gives it 120 seconds. A second signal still kills it at once. |
| P4 | The worker ran the diagnostic agent after a failure with no time limit | A slow or unreachable model held the worker, and the webhook, for as long as it liked. | The agent is cut off after `DIAGNOSTIC_TIMEOUT_SECONDS` (120). The run goes on without a recommendation and the webhook is sent. |
| P5 | The agent's libraries were imported in the middle of the first failed run | On a slow machine the import took 54 seconds, the whole event loop stood still, and the lease heartbeat could not run. | They are imported once at worker startup, before any run is taken. A broken import is logged and does not stop the worker. |
| P6 | A pipeline's source URL and webhook URL were called as written | A tenant could point either at `169.254.169.254` (the cloud metadata service, which hands out the node's credentials), at Postgres, at Redis or at any service inside the cluster, and read the answer in the run's error or the saved file. There was no limit on the size of a download either. | Private, loopback, link-local and reserved addresses are refused, at the moment of connecting. See below. Downloads stop at `MAX_DOWNLOAD_MB` (100). |
| P7 | The images | Ran as root, and carried a C compiler and libpq headers that nothing needed. | Run as user 10001 and have no compiler. The chart refuses to start them as root. |
| P8 | The schema was changed by hand before an install (`helm/shdp/README.md` said so) | A new worker could start on a database that lacked its columns. | A Helm hook Job runs `alembic upgrade head` before every install and upgrade, and the release fails if it fails. |
| P9 | A change to the chart's config or secrets | The ConfigMap changed and the pods kept running with the old value. Found on the cluster: an upgrade switched `ALLOW_PRIVATE_URLS` on and the pods still refused. | The pod templates carry a checksum of the config and the secrets, so a change replaces the pods. |

## The private-address check

A tenant chooses two URLs: where an ingestion step downloads from, and where the webhook goes. The worker runs inside the cluster, so those URLs are a way to make the worker call things the tenant cannot reach.

[`shared/url_guard.py`](../shared/url_guard.py) does four things:

- Only `http` and `https`, with a host. This is also checked when a pipeline is saved, so a bad `callback_url` is a 422 at once.
- The name is looked up and **every** address it gives must be public. A name that mixes a public and a private answer is refused. An IPv6 address that wraps an IPv4 one (`::ffff:169.254.169.254`, 6to4, NAT64) is judged by the IPv4 inside it. The odd ways to write loopback (`2130706433`, `0x7f000001`, `127.1`) are refused too, because the check is on the address the operating system's resolver returns, not on the text.
- The connection is made to the address that was checked. The name is looked up once per connection and that answer is used, so a name that answers with a public address first and a private one next (DNS rebinding) gets nothing. TLS still checks the certificate against the name in the URL.
- Redirects are not followed and proxy settings in the environment are ignored.

`ALLOW_PRIVATE_URLS=true` turns the address check off, for a developer machine (the examples and tests use a source on `127.0.0.1`) or a cluster where every tenant is trusted. It is `false` in the chart and `true` in `.env.example`.

How it was checked: unit tests (5 public and 18 non-public addresses, including the wrapped IPv6 forms, and the odd spellings of loopback sent through the real resolver), the real client against a real local server (refused before any connection, reached when allowed, connected to the looked-up address, a redirect to the metadata address not followed, a download over the limit stopped both by its declared size and while reading an answer of unknown size), and the end-to-end test below, where a locked-down platform refuses a source and a webhook on `127.0.0.1` and records why.

The client reaches into `httpx`'s connection pool to install the checking network backend, because `httpx` has no public way to set it. `httpx` and `httpcore` are pinned in `requirements.txt` and the tests fail if that stops working.

## How it was checked

- **Unit tests**: 118 new, 157 to 275 (configuration, readiness, shutdown, the agent timeout, the address check). They run in CI.
- **Real-process tests**: the worker is started as a real program and sent a stop signal while it works on a run. It finishes the run (`success`, `attempts = 1`), leaves the next run on the queue, and exits with code 0. The same test against the old worker fails, because the old worker was killed (exit code `0xC000013A` on Windows).
- **End-to-end tests** ([`tests/integration/test_end_to_end.py`](../tests/integration/test_end_to_end.py)): the real API and the real worker as two separate programs, a real Postgres and Redis, and a data source and webhook receiver on real sockets, with everything done through HTTP:
  - create a tenant, build a four-step pipeline, run it, and read the finished data from the warehouse;
  - the webhook arrives once, and a repeated request with the same `Idempotency-Key` returns the same run and runs nothing twice;
  - a failing source fails the run with its reason and sends a `run.failed` webhook;
  - a missing key is refused, a wrong key is refused, another tenant's key is refused, a wrong admin secret is refused;
  - a webhook that is not an http URL is a 422;
  - `/health`, `/ready` and `/metrics`;
  - a platform started without private addresses refuses a source and a webhook on `127.0.0.1` and records the reason;
  - both programs refuse to start without a secret, and in production with a weak one.
- **Images and chart**: checked on a local Kubernetes cluster, see below.

Two things the end-to-end tests found that the unit tests had not: the diagnostic agent had no time limit (P4), and its import blocked the worker for 54 seconds (P5). The end-to-end run time went from about 200 seconds to 60 seconds with the second fix.

## Checked on a cluster

The chart and both images were installed on a local minikube cluster (not EKS) with Postgres, Redis and a small data source as pods. `ENV=production` was set, so the secrets rule was enforced at startup.

- **Install**: the migration hook Job ran on an empty database and brought it to the latest revision, then it and its Secret were removed. Both pods run as uid 10001.
- **Private addresses**: with `ALLOW_PRIVATE_URLS=false`, pipelines whose source was an in-cluster service (`http://source:8080`), the metadata address (`169.254.169.254`) and Redis all failed with "points to a private or internal address".
- **Readiness**: with the Redis pod deleted, `/ready` answered 503, `/health` stayed 200, the control plane pod went `NotReady` and left the Service, and its restart count stayed 0. With Redis back it was ready again.
- **Upgrade**: the migration Job ran again without harm. **This found P9**: the first upgrade changed the ConfigMap and the pods did not restart, so they kept the old value. The checksum annotations fixed it; the next upgrade replaced the pods and a pipeline on the in-cluster source then ran to `success`.
- **Graceful stop**: a four-second run was started and the worker pod deleted while it ran. The log shows `worker_shutdown_requested`, the remaining steps finishing, then `worker_stopped`; the run ended `success` with `attempts = 1`, and the pod took 6 seconds to go, not the 120 allowed.

Not checked: EKS itself (managed node groups, ECR pulls, IAM), more than one replica of each, and an upgrade while runs are in flight.

## Settings added

| Variable | Default | Meaning |
|---|---|---|
| `ALLOW_PRIVATE_URLS` | `false` | Allow sources and webhooks on private addresses. Only for development or trusted tenants. |
| `MAX_DOWNLOAD_MB` | `100` | The largest file an ingestion step downloads. |
| `DIAGNOSTIC_TIMEOUT_SECONDS` | `120` | How long the diagnostic agent may take before the run goes on without it. |

## What is still not done

- **Authentication is one shared secret per kind.** The admin secret is a single value, and API keys have no expiry, scopes or rotation of the signing secret. Changing `API_KEY_SECRET` invalidates every key.
- **No rate limiting.** One tenant can fill the queue. A limit per tenant at the API, or a bound on the queue, is needed before untrusted tenants.
- **No TLS and no ingress in the chart.** The Service is `ClusterIP`. Terminating TLS and exposing it is left to whoever deploys it.
- **No autoscaling and no pod disruption budget.** Workers are scaled by hand (`worker.replicaCount`). An autoscaler would need queue depth as a metric.
- **The warehouse and the run files are local.** The `load` step writes to the one Postgres warehouse, and files go under `/app/data` in the pod, which is lost when the pod is. The files are only read by later steps of the same run, so this loses nothing but the `run_context.json` copy.
- **Checking a rebinding-safe address needs a direct connection.** If a cluster forces all traffic through an egress proxy, the check sees the proxy, not the destination. Put the restriction in the network policy of that cluster as well. No network policy is shipped in the chart.
- **Not run on AWS since these changes.** The chart was checked on a local cluster (below), not on EKS.
- **The agent's prompt problems** listed in [ROADMAP.md](ROADMAP.md) are unchanged.
