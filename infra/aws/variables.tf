variable "aws_region" {
  description = "AWS region for the EKS cluster and its dependencies."
  type        = string
  default     = "us-west-2" # chosen over us-east-2 (the account's CLI default) for full AgentCore/Bedrock support, per the portfolio-wide region decision
}

variable "project_name" {
  description = "Short name used as a prefix on every resource this stack creates, so it's trivially greppable in the AWS console before teardown and impossible to confuse with anything else in the account."
  type        = string
  default     = "shdp-portfolio-test"
}

variable "db_password" {
  description = "RDS master password. Passed via TF_VAR_db_password, never committed or hardcoded."
  type        = string
  sensitive   = true
}

variable "admin_secret" {
  description = "Control plane's ADMIN_SECRET_KEY. Passed via TF_VAR_admin_secret."
  type        = string
  sensitive   = true
}

variable "api_key_secret" {
  description = "Control plane's API_KEY_SECRET (used to hash tenant API keys). Passed via TF_VAR_api_key_secret."
  type        = string
  sensitive   = true
}

variable "google_api_key" {
  description = "GOOGLE_API_KEY for the diagnostic agent's Gemini calls. Passed via TF_VAR_google_api_key."
  type        = string
  sensitive   = true
}
