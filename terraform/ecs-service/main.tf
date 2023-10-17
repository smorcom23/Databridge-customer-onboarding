terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "4.45.0"
    }
  }
  required_version = ">= 1.5.0"
}

provider "aws" {
  region = var.aws_region
}

#######################################
# VPC/subnets/certificate retrieved from parameter store
#######################################
data "aws_ssm_parameter" "vpc_id" {
  name = "/AG/base/vpcId"
}

data "aws_ssm_parameter" "private_subnet_a_id" {
  name = "/AG/base/privateSubnetA"
}

data "aws_ssm_parameter" "private_subnet_b_id" {
  name = "/AG/base/privateSubnetB"
}

data "aws_ssm_parameter" "private_subnet_c_id" {
  name = "/AG/base/privateSubnetC"
}

data "aws_ssm_parameter" "public_subnet_a_id" {
  name = "/AG/base/publicSubnetA"
}

data "aws_ssm_parameter" "public_subnet_b_id" {
  name = "/AG/base/publicSubnetB"
}

data "aws_ssm_parameter" "public_subnet_c_id" {
  name = "/AG/base/publicSubnetC"
}

data "aws_ssm_parameter" "cert" {
  name = "/AG/base/agcloudIrelandCertificateArn"
}

#data "aws_vpc" "vpc" {
#  id = data.aws_ssm_parameter.vpc_id.value
#}

module "cluster_alb" {
  source = "github.com/arisglobal/sharedactions//terraform/modules/ecs/cluster_alb?ref=latest"

  deploy_env      = var.deploy_env
  product         = var.product
  product_version = var.product_version
  customer_name   = var.customer_name
  revenue_type    = var.revenue_type
  requestor_name  = var.requestor_name
  jira_id         = var.jira_id

  create_cluster = var.create_cluster
  cluster_name   = var.cluster_name
  vpc_id         = data.aws_ssm_parameter.vpc_id.value
  public_subnets = [
    data.aws_ssm_parameter.public_subnet_a_id.value,
    data.aws_ssm_parameter.public_subnet_b_id.value,
    data.aws_ssm_parameter.public_subnet_c_id.value
  ]
  alb_name                  = "${var.service_prefix}-alb"
  target_group_name         = "${var.service_prefix}-tgt-group"
#  ecs_service_sg_name       = "${var.service_prefix}-ecs-svc-sg"
  service_port_target_group = var.service_port_target_group
  context_path              = var.context_path
  healthcheck_path          = var.healthcheck_path
  alb_https_certificate_arn = data.aws_ssm_parameter.cert.value

  deploy_repo    = var.deploy_repo
  pipeline_token = var.pipeline_token
}

module "fargate" {
  source = "github.com/arisglobal/sharedactions//terraform/modules/ecs/service_taskdef?ref=latest"

  deploy_env      = var.deploy_env
  product         = var.product
  product_version = var.product_version
  customer_name   = var.customer_name
  revenue_type    = var.revenue_type
  requestor_name  = var.requestor_name
  jira_id         = var.jira_id

  cluster_name    = var.cluster_name
  vpc_id          = data.aws_ssm_parameter.vpc_id.value
#  vpc_cidr_block  = data.aws_vpc.vpc.cidr_block
  private_subnets = [
    data.aws_ssm_parameter.private_subnet_a_id.value,
    data.aws_ssm_parameter.private_subnet_b_id.value,
    data.aws_ssm_parameter.private_subnet_c_id.value
  ]
  alb_security_group_id              = module.cluster_alb.alb_security_group_id
  alb_target_group_arn               = module.cluster_alb.ecs_alb_target_group_arn
  cpu                                = var.cpu
  memory                             = var.memory
  service_name                       = var.service_name
#  ecs_service_sg_name                = "${var.service_prefix}-ecs-svc-sg"
  ecr_repository_name                = var.ecr_repository_name
  service_port_target_group          = var.service_port_target_group
  log_group_retention_in_days        = var.log_group_retention_in_days

  deploy_repo    = var.deploy_repo
  pipeline_token = var.pipeline_token
}
