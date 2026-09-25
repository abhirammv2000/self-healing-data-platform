# Ingress is scoped to the VPC CIDR (10.42.0.0/16) rather than the EKS module's specific
# pod/node security group, since that group's exact name wasn't confirmed up front and too
# narrow a rule would silently break connectivity during verification. A production setup
# would tighten this to the EKS node security group specifically.

resource "aws_security_group" "rds" {
  name_prefix = "${var.project_name}-rds-"
  description = "Allow Postgres from inside the VPC only"
  vpc_id      = module.vpc.vpc_id

  ingress {
    description = "Postgres from within the VPC"
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = [module.vpc.vpc_cidr_block]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = local.tags
}

resource "aws_security_group" "redis" {
  name_prefix = "${var.project_name}-redis-"
  description = "Allow Redis from inside the VPC only"
  vpc_id      = module.vpc.vpc_id

  ingress {
    description = "Redis from within the VPC"
    from_port   = 6379
    to_port     = 6379
    protocol    = "tcp"
    cidr_blocks = [module.vpc.vpc_cidr_block]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = local.tags
}
