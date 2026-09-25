# Deploying to EKS

This chart deploys the control plane and worker to Kubernetes. It was built and tested
against an EKS Auto Mode cluster provisioned by `infra/aws/` (Terraform), then torn down,
see the root README's Observability/Infra section for the full story. There's no cluster
running against it by default.

## Prerequisites

- `infra/aws/` applied (`terraform apply`), producing an EKS cluster, RDS
  Postgres, ElastiCache Redis, and two ECR repos.
- `kubectl` pointed at the cluster: `aws eks update-kubeconfig --name <cluster_name> --profile eks-portfolio`
- Control plane and worker images built and pushed to the ECR repos from
  `terraform output`.
- Helm 3.

## Secrets

`values.yaml` ships with no hosts or credentials. Create a **gitignored**
`secrets.local.yaml` next to this README with the values before installing:

```yaml
secrets:
  databaseUrl: "postgresql+asyncpg://<user>:<pass>@<rds_endpoint>:5432/shdp"
  databaseUrlSync: "postgresql+psycopg2://<user>:<pass>@<rds_endpoint>:5432/shdp"
  dataWarehouseUrl: "postgresql+asyncpg://<user>:<pass>@<rds_endpoint>:5432/data_warehouse"
  redisUrl: "redis://<elasticache_endpoint>:6379/0"
  apiKeySecret: "<random-string>"
  adminSecretKey: "<random-string>"
  googleApiKey: "<Gemini API key>"
```

`data_warehouse` is a second database on the same RDS instance. It has to be
created from inside the VPC (RDS isn't publicly accessible), e.g. from a
throwaway pod:

```
kubectl run psql-client --rm -it --image=postgres:17 --restart=Never -- \
  psql "postgresql://<user>:<pass>@<rds_endpoint>:5432/postgres" \
  -c "CREATE DATABASE data_warehouse;"
```

Then run Alembic migrations against both databases before installing the chart
(from a machine/pod that can reach RDS, or a one-off `kubectl run` job using
the control-plane image).

## Install

```
helm install shdp . \
  -f secrets.local.yaml \
  --set image.controlPlane.repository=<terraform output ecr_control_plane_repo> \
  --set image.worker.repository=<terraform output ecr_worker_repo>
```

## Verify

The control-plane Service is `ClusterIP`, not `LoadBalancer`. A `LoadBalancer`
Service on EKS provisions an AWS ELB/NLB outside Terraform's state, and
forgetting to delete it before `terraform destroy` leaves it orphaned and
billing. Verify with:

```
kubectl port-forward svc/shdp-control-plane 8000:80
curl http://localhost:8000/health
```

Then run a pipeline through the port-forwarded API and confirm it completes
via `kubectl logs` on the worker pod.

`OTEL_TRACES_ENABLED` is `false` here; Jaeger/Prometheus/Grafana aren't
deployed to the cluster. Those were already tested against the local Docker
Compose stack, so this deploy is only about proving the app runs on
EKS/RDS/ElastiCache.

## Teardown

Before `terraform destroy`, remove anything Terraform doesn't track:

```
helm uninstall shdp
```

Since the Service is `ClusterIP`, there's no load balancer to separately
clean up. After `helm uninstall`, run `terraform destroy` in `infra/aws/` and
confirm via the AWS Console/CLI that no billable resources remain (EBS
volumes, Elastic IPs, NAT gateways, the RDS instance, the ElastiCache cluster,
ECR repos, the EKS cluster itself).
