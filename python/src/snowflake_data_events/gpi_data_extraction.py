#!/usr/bin/env python
import os
import boto3
import botocore
from python_util.snowflake_connector_util import snowflake_connect
from python_util.snowflake_connector_util import execute_sql_file_stream
from python_util.aws_util import get_stored_params


# Default lambda handler to connect to snowflake
def gpi_data_extraction(object_key_prefix, sql_script, input_params):
    sql_filepath = "../sql/"

    # Get snowflake params from AWS to connect to snowflake
    snowflake_params = None
    try:
        snowflake_params = get_stored_params()
    except botocore.exceptions.ClientError as e:
        print('Could not fetch credentials, possibly running the test suite %s', e)
    except Exception as e:
        raise Exception('Could not fetch parameters from SSM and secrets manager, Perhaps you are not authenticated. %s',
                        e)

    conn = snowflake_connect(snowflake_params)
    output_filename = execute_sql_file_stream(conn, snowflake_params, sql_filepath + sql_script, input_params)
    conn.close()
    
    return output_filename
