bucket         = "terraform-remote-backend-qc"
key            = "snowflake_dev/state.tfstate"
region         = "eu-west-1"
encrypt        = "true"
dynamodb_table = "terraform_state_lock"

