aws_region          = "eu-west-1"

#######################################
# Define apps and tenants, append to the
# list for new apps/tenants, comma delimited.
# DO NOT REMOVE ANY EXISTING APPS/TENANTS UNLESS INTENTIONAL!
# Removing apps/tenants will destroy their Snowflake resources!
#######################################
app_tenant = [
  "databridge_terratest1" #,
  #"databridge_terratest2"
]

#######################################
# user variables
#######################################
user_map = {
  "STDRPT" = {
    "first_name"        = "test",
    "last_name"         = "user1",
    "email"             = "test@test.com",
    "default_warehouse" = "COMPUTE_WH",
    "default_role"      = "PUBLIC",
    "privilege"         = "ALL PRIVILEGES"
  },
  #"ETL" = {
   # "first_name"        = "test",
    #"last_name"         = "user2",
    #"email"             = "test@test2.com",
    #"default_warehouse" = "COMPUTE_WH",
    #"default_role"      = "PUBLIC",
    #"privilege"         = "ALL PRIVILEGES"
  #},
}

#######################################
# role variables
#######################################
role_map = {
  "RO_ROLE" = {
    "role_type"    = "RO",
    "role_comment" = "name1 comment",
    "role_names"   = ["RW_ROLE","SECURITYADMIN"],
    "users"        = ["STDRPT"]
  },
  "RW_ROLE" = {
    "role_type"    = "RW",
    "role_comment" = "name2 comment",
    "role_names"   = [],
    "users"        = ["STDRPT","MPATIL@ARISGLOBAL.COM"]  #use the complete name of service user
  },
}

#######################################
# warehouse variables
#######################################
warehouse_map = {
  "STDRPT_XSMALL_WH" = {
    "key"                 = "STDRPT",
    "warehouse_size"        = "XSMALL",
    "max_cluster_count"     = 1,
    "auto_suspend"          = 60
  },
  "ETL_XSMALL_WH" = {
    "key"                 = "ETL",
    "warehouse_size"        = "XSMALL",
    "max_cluster_count"     = 1,
    "auto_suspend"          = 60
  },
}

warehouse_role_grants = {
  "STDRPT_XSMALL_WH USAGE" =  ["RO_ROLE", "RW_ROLE"],
  "STDRPT_XSMALL_WH OWNERSHIP" = ["SYSADMIN"],
  "ETL_XSMALL_WH USAGE" = ["RW_ROLE"]
  "ETL_XSMALL_WH OWNERSHIP" = ["SYSADMIN"]
}

#######################################
# database variables
#######################################
create_db                      = true
db_comment                     = "test db"
db_data_retention_time_in_days = 1

db_role_grants = {
  "USAGE"     = ["RO_ROLE","RW_ROLE"],
  "OWNERSHIP" = ["SYSADMIN"]
}

schemas       = ["LSDB_RPL","LSDB_TRANSFM"]
schema_grants = {
  "LSDB_TRANSFM USAGE"        = { "roles" = ["RO_ROLE"] },
  "LSDB_RPL CREATE TABLE" = { "roles" = ["RW_ROLE"] },
  "LSDB_RPL USAGE"        = { "roles" = ["RO_ROLE"] },
  "LSDB_TRANSFM CREATE TABLE" = { "roles" = ["RW_ROLE"] },
  "LSDB_TRANSFM OWNERSHIP" = { "roles" = ["SYSADMIN"] },
  "LSDB_RPL OWNERSHIP" = { "roles" = ["SYSADMIN"] },
}

role_override = ["SYSADMIN","SECURITYADMIN","COOMON_RO_ROLE"]

user_override = ["MPATIL@ARISGLOBAL.COM"]