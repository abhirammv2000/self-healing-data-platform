output "cluster_name" {
  value = module.eks.cluster_name
}

output "configure_kubectl" {
  description = "Run this to point kubectl at the new cluster."
  value       = "aws eks update-kubeconfig --name ${module.eks.cluster_name} --region ${var.aws_region} --profile eks-portfolio"
}

output "rds_endpoint" {
  value = aws_db_instance.postgres.address
}

output "redis_endpoint" {
  value = aws_elasticache_cluster.redis.cache_nodes[0].address
}

output "ecr_control_plane_repo" {
  value = aws_ecr_repository.control_plane.repository_url
}

output "ecr_worker_repo" {
  value = aws_ecr_repository.worker.repository_url
}

output "ecr_login_command" {
  value = "aws ecr get-login-password --region ${var.aws_region} --profile eks-portfolio | docker login --username AWS --password-stdin ${aws_ecr_repository.control_plane.repository_url}"
}
