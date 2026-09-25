# Dedicated VPC for this test rather than the account's existing default VPC
# (vpc-0df2ff91dc4568c57), so `terraform destroy` removes everything this stack creates
# without risk of touching anything already in the account.

module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "~> 6.7"

  name = "${var.project_name}-vpc"
  cidr = "10.42.0.0/16"

  azs             = ["${var.aws_region}a", "${var.aws_region}b"]
  private_subnets = ["10.42.0.0/20", "10.42.16.0/20"]    # Fargate pods, RDS, ElastiCache
  public_subnets  = ["10.42.128.0/20", "10.42.144.0/20"] # NAT gateway, internet-facing LB if ever needed

  enable_nat_gateway   = true
  single_nat_gateway   = true # one NAT for both AZs; it's the biggest line-item cost in the stack ($0.045/hr + data processing), and this short-lived test doesn't need AZ-redundant NAT
  enable_dns_hostnames = true
  enable_dns_support   = true

  # EKS-required tags: the cluster autoscaler and AWS Load Balancer Controller discover subnets
  # by these tags rather than by name. Included even though this stack uses neither (see
  # helm/README), since they're free to add and any EKS deployment needs them.
  public_subnet_tags = {
    "kubernetes.io/role/elb" = "1"
  }
  private_subnet_tags = {
    "kubernetes.io/role/internal-elb"             = "1"
    "kubernetes.io/cluster/${local.cluster_name}" = "shared"
  }

  tags = local.tags
}

locals {
  cluster_name = "${var.project_name}-eks"
  tags = {
    Project   = var.project_name
    ManagedBy = "terraform"
    Purpose   = "portfolio-verification-deploy-then-destroy"
  }
}
