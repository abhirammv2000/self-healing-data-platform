resource "aws_elasticache_subnet_group" "this" {
  name       = "${var.project_name}-redis"
  subnet_ids = module.vpc.private_subnets
}

resource "aws_elasticache_cluster" "redis" {
  cluster_id           = "${var.project_name}-redis"
  engine               = "redis"
  node_type            = "cache.t4g.micro" # smallest Graviton cache node, same reasoning as RDS's instance class
  num_cache_nodes      = 1                 # single node, no replication: a short-lived test doesn't need HA
  parameter_group_name = "default.redis7"
  engine_version       = "7.1"
  port                 = 6379

  subnet_group_name  = aws_elasticache_subnet_group.this.name
  security_group_ids = [aws_security_group.redis.id]

  tags = local.tags
}
