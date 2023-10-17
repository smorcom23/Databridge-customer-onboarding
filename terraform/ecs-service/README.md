<!-- BEGIN_TF_DOCS -->
## Requirements

| Name | Version |
|------|---------|
| <a name="requirement_terraform"></a> [terraform](#requirement\_terraform) | ~> 1.4.0 |
| <a name="requirement_aws"></a> [aws](#requirement\_aws) | 4.45.0 |

## Providers

| Name | Version |
|------|---------|
| <a name="provider_aws"></a> [aws](#provider\_aws) | 4.45.0 |

## Modules

| Name | Source | Version |
|------|--------|---------|
| <a name="module_ecs_service"></a> [ecs\_service](#module\_ecs\_service) | github.com/arisglobal/sharedactions//terraform/modules/ag-api/service | latest |

## Resources

| Name | Type |
|------|------|
| [aws_ssm_parameter.ag_api_alb_security_group_id](https://registry.terraform.io/providers/hashicorp/aws/4.45.0/docs/data-sources/ssm_parameter) | data source |
| [aws_ssm_parameter.ag_api_apigateway_id](https://registry.terraform.io/providers/hashicorp/aws/4.45.0/docs/data-sources/ssm_parameter) | data source |
| [aws_ssm_parameter.ag_api_lambda_authorizer_id](https://registry.terraform.io/providers/hashicorp/aws/4.45.0/docs/data-sources/ssm_parameter) | data source |
| [aws_ssm_parameter.alb_arn_suffix](https://registry.terraform.io/providers/hashicorp/aws/4.45.0/docs/data-sources/ssm_parameter) | data source |
| [aws_ssm_parameter.alb_listener_arn](https://registry.terraform.io/providers/hashicorp/aws/4.45.0/docs/data-sources/ssm_parameter) | data source |
| [aws_ssm_parameter.apigateway_integration_id](https://registry.terraform.io/providers/hashicorp/aws/4.45.0/docs/data-sources/ssm_parameter) | data source |
| [aws_ssm_parameter.private_subnet_a_id](https://registry.terraform.io/providers/hashicorp/aws/4.45.0/docs/data-sources/ssm_parameter) | data source |
| [aws_ssm_parameter.private_subnet_b_id](https://registry.terraform.io/providers/hashicorp/aws/4.45.0/docs/data-sources/ssm_parameter) | data source |
| [aws_ssm_parameter.private_subnet_c_id](https://registry.terraform.io/providers/hashicorp/aws/4.45.0/docs/data-sources/ssm_parameter) | data source |
| [aws_ssm_parameter.vpc_id](https://registry.terraform.io/providers/hashicorp/aws/4.45.0/docs/data-sources/ssm_parameter) | data source |

## Inputs

| Name | Description | Type | Default | Required |
|------|-------------|------|---------|:--------:|
| <a name="input_alb_request_count_per_target"></a> [alb\_request\_count\_per\_target](#input\_alb\_request\_count\_per\_target) | ALB request count per target, used for target\_tracking\_scaling\_policy\_configuration | `string` | n/a | yes |
| <a name="input_aws_region"></a> [aws\_region](#input\_aws\_region) | AWS region | `string` | n/a | yes |
| <a name="input_context_path"></a> [context\_path](#input\_context\_path) | application's path, used for ALB listener rule configuration | `string` | n/a | yes |
| <a name="input_cpu"></a> [cpu](#input\_cpu) | The CPU size | `string` | `"512"` | no |
| <a name="input_customer_name"></a> [customer\_name](#input\_customer\_name) | customer name tag | `string` | `"epd"` | no |
| <a name="input_deploy_env"></a> [deploy\_env](#input\_deploy\_env) | CI injected variable, deployment environment | `string` | `"dev"` | no |
| <a name="input_deploy_repo"></a> [deploy\_repo](#input\_deploy\_repo) | CI injected variable, application's repo name | `string` | `"sharedactions"` | no |
| <a name="input_ecr_repository_name"></a> [ecr\_repository\_name](#input\_ecr\_repository\_name) | The ECR repository name | `string` | n/a | yes |
| <a name="input_ecs_autoscaling_target_max_capacity"></a> [ecs\_autoscaling\_target\_max\_capacity](#input\_ecs\_autoscaling\_target\_max\_capacity) | ecs autoscaling target max\_capacity | `number` | n/a | yes |
| <a name="input_ecs_autoscaling_target_min_capacity"></a> [ecs\_autoscaling\_target\_min\_capacity](#input\_ecs\_autoscaling\_target\_min\_capacity) | ecs autoscaling target min\_capacity | `number` | n/a | yes |
| <a name="input_healthcheck_path"></a> [healthcheck\_path](#input\_healthcheck\_path) | application's health check path | `string` | n/a | yes |
| <a name="input_http_api_gateway_name"></a> [http\_api\_gateway\_name](#input\_http\_api\_gateway\_name) | HTTP API Gateway name | `string` | n/a | yes |
| <a name="input_jira_id"></a> [jira\_id](#input\_jira\_id) | jira ticket id tag | `string` | `""` | no |
| <a name="input_log_group_retention_in_days"></a> [log\_group\_retention\_in\_days](#input\_log\_group\_retention\_in\_days) | Specifies the number of days you want to retain log events in the specified log group. Possible values are: 1, 3, 5, 7, 14, 30, 60, 90, 120, 150, 180, 365, etc. | `number` | `7` | no |
| <a name="input_memory"></a> [memory](#input\_memory) | The memory size | `string` | `"1024"` | no |
| <a name="input_parameter_store_entries"></a> [parameter\_store\_entries](#input\_parameter\_store\_entries) | Map of parameter store entries | `map(any)` | `{}` | no |
| <a name="input_pipeline_token"></a> [pipeline\_token](#input\_pipeline\_token) | CI injected variable, pipeline token | `string` | n/a | yes |
| <a name="input_product"></a> [product](#input\_product) | project name tag | `string` | `"plat"` | no |
| <a name="input_product_version"></a> [product\_version](#input\_product\_version) | product version tag | `string` | `"plat2023.1.0.0"` | no |
| <a name="input_requestor_name"></a> [requestor\_name](#input\_requestor\_name) | requestor name tag | `string` | `""` | no |
| <a name="input_revenue_type"></a> [revenue\_type](#input\_revenue\_type) | revenue type tag | `string` | `"non-rev"` | no |
| <a name="input_service_name"></a> [service\_name](#input\_service\_name) | service name, the same as the ECR image name | `string` | n/a | yes |
| <a name="input_service_port_target_group"></a> [service\_port\_target\_group](#input\_service\_port\_target\_group) | application's service port | `number` | `8080` | no |

## Outputs

| Name | Description |
|------|-------------|
| <a name="output_container_name"></a> [container\_name](#output\_container\_name) | n/a |
| <a name="output_ecr_repository_name"></a> [ecr\_repository\_name](#output\_ecr\_repository\_name) | n/a |
| <a name="output_ecs_service"></a> [ecs\_service](#output\_ecs\_service) | n/a |
| <a name="output_ecs_task_definition"></a> [ecs\_task\_definition](#output\_ecs\_task\_definition) | n/a |
| <a name="output_fargate_task_security_group_id"></a> [fargate\_task\_security\_group\_id](#output\_fargate\_task\_security\_group\_id) | n/a |
<!-- END_TF_DOCS -->