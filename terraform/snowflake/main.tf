terraform {
  required_providers {
    snowflake = {
      source  = "Snowflake-Labs/snowflake"
      version = "~> 0.69"
    }
  }
  required_version = ">= 1.5.0"
}

provider "snowflake" {
  role     = "terraform_role"
  account  = var.snowflake_account
  region   = var.snowflake_region
  username = var.snowflake_user
  password = var.snowflake_password
}

provider "aws" {
  region = var.aws_region
}

#######################################
# Call snowflake composite module, loop through app_tenant to create Snowflake resources.
# Create separate module to handle exception cases where standard template doesn't apply.
#######################################
module "snowflake" {
  source = "git@github.com:smorcom23/Databridge-customer-onboarding.git/terraform/modules/snowflake_latest?ref=latest"
#  source = "github.com/smorcom23/Databridge-customer-onboarding//terraform/modules/snowflake_latest?ref=latest"

  for_each = {
    for pair in var.app_tenant :
      pair => {
        app    = split("_", pair)[0]
        tenant = split("_", pair)[1]
      }
  }

  ag_application_name   = each.value.app
  customer_name         = each.value.tenant

  user_map              = var.user_map
  role_map              = var.role_map
  warehouse_map         = var.warehouse_map
  warehouse_role_grants = var.warehouse_role_grants

  create_db                      = var.create_db
  db_comment                     = var.db_comment
  db_data_retention_time_in_days = var.db_data_retention_time_in_days
  db_role_grants                 = var.db_role_grants
  schemas                        = var.schemas
  schema_grants                  = var.schema_grants
  snowflake_account              = var.snowflake_account
  role_override                  = var.role_override
  user_override                  = var.user_override
}