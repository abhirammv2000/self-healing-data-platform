# Uses EKS Auto Mode (module v21's "name"/"kubernetes_version" inputs, replacing the older
# "cluster_name"/"cluster_version"). AWS handles node provisioning and scaling directly, so
# there's no node group or Fargate profile to size or clean up.

module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "~> 21.0"

  name                   = local.cluster_name
  kubernetes_version     = "1.34"
  endpoint_public_access = true # simplest for this short-lived test; a long-lived deployment would restrict this to a VPN/bastion

  enable_cluster_creator_admin_permissions = true # gives the applying identity kubectl admin access via an EKS access entry, no aws-auth ConfigMap needed

  compute_config = {
    enabled    = true
    node_pools = ["general-purpose"]
  }

  vpc_id     = module.vpc.vpc_id
  subnet_ids = module.vpc.private_subnets

  tags = local.tags
}
