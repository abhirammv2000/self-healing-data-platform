resource "aws_ecr_repository" "control_plane" {
  name                 = "${var.project_name}/control-plane"
  image_tag_mutability = "MUTABLE"
  force_delete         = true # lets terraform destroy remove the repo even if it still has images

  image_scanning_configuration {
    scan_on_push = true
  }

  tags = local.tags
}

resource "aws_ecr_repository" "worker" {
  name                 = "${var.project_name}/worker"
  image_tag_mutability = "MUTABLE"
  force_delete         = true

  image_scanning_configuration {
    scan_on_push = true
  }

  tags = local.tags
}
