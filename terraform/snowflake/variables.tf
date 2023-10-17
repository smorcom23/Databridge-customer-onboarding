variable "snowflake_account" {
  description = "Snowflake account"
  type        = string
}

variable "snowflake_region" {
  description = "Snowflake region"
  type        = string
}

variable "snowflake_user" {
  description = "Snowflake user"
  type        = string
}

variable "snowflake_password" {
  description = "Snowflake password"
  type        = string
}

variable "aws_region" {
  description = "AWS region for SSM"
  type        = string
}

variable "app_tenant" {
  description = "App/tenant mapping"
  type        = list(string)
}

variable "user_map" {
  description = "user map"
  type        = map(any)
}

variable "role_map" {
  description = "role map"
  type        = map(any)
}

variable "warehouse_map" {
  description = "warehouse map"
  type        = map(any)
}

variable "warehouse_role_grants" {
  description = "map of warehouse roles and their corresponding grants"
  type        = map(any)
}

variable "create_db" {
  description = "flag whether or not to create db"
  type        = bool
}

variable "db_comment" {
  description = "DB comment"
  type        = string
}

variable "db_data_retention_time_in_days" {
  description = "DB data retention in days"
  type        = number
}

variable "db_role_grants" {
  description = "map of DB roles and their corresponding grants"
  type        = map(any)
}

variable "schemas" {
  description = "a list of schemas"
  type        = list(string)
}

variable "schema_grants" {
  description = "schema grants"
  type        = map(any)
}

variable "role_override" {
  description = "Predefined roles to override grants"
  type        = list(string)
}

variable "user_override" {
  description = "Predefined users to override role grants"
  type        = list(string)
}