# RDS Postgres 17.x supports pgvector 0.8.2, matching the pgvector/pgvector:pg17 image used
# locally in docker-compose.yml, so the same migrations (including `CREATE EXTENSION vector`)
# apply unchanged.

resource "aws_db_subnet_group" "this" {
  name       = "${var.project_name}-db"
  subnet_ids = module.vpc.private_subnets
  tags       = local.tags
}

resource "aws_db_instance" "postgres" {
  identifier     = "${var.project_name}-pg"
  engine         = "postgres"
  engine_version = "17.11"        # 17.4 isn't offered by RDS in us-west-2 (only 17.5-17.11 are); pgvector support isn't gated by patch version, so this doesn't affect the extension
  instance_class = "db.t4g.micro" # smallest Graviton instance class; this is a verify-then-destroy test, not a sized-for-load deployment

  allocated_storage = 20 # GB, the minimum RDS allows for Postgres
  storage_type      = "gp3"
  storage_encrypted = true

  db_name  = "data_platform_db"
  username = "postgres"
  password = var.db_password

  db_subnet_group_name   = aws_db_subnet_group.this.name
  vpc_security_group_ids = [aws_security_group.rds.id]

  multi_az            = false # single-AZ: a short-lived test has no reason to pay for standby redundancy
  publicly_accessible = false
  skip_final_snapshot = true  # no snapshot needed for infrastructure that's being destroyed on purpose
  deletion_protection = false # must be false, or `terraform destroy` would fail outright

  tags = local.tags
}

# The second database (the load step's "data warehouse" target) lives on this same RDS
# instance, mirroring docker-compose.yml's init-2nd-db.sql locally.
#
# It isn't created via a local-exec provisioner: publicly_accessible is false, so it's only
# reachable from inside the VPC, and `terraform apply` runs outside it. Instead it's created
# from a one-off pod inside the EKS cluster after it comes up, in the same step that verifies
# connectivity (see infra/aws/README.md's deploy runbook).
