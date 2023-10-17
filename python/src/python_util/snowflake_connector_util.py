#!/usr/bin/env python
import os
import datetime
import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization


# Function to create connection object for Snowflake
def snowflake_connect(snowflake_params):

    # Want to use key-pair auth to connect to snowflake
    p_key = serialization.load_pem_private_key(
        snowflake_params.get('private_key_value').encode(),  # This accesses our private key from secrets mngr
        password=snowflake_params.get('private_key_passphrase').encode(),
        backend=default_backend()
    )

    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption())

    # Note: We can specify the db and schema/warehouse here instead of pointing to them using sql executions
    ctx = snowflake.connector.connect(
        user=snowflake_params.get('username'),
        account=snowflake_params.get('account_name'),
        private_key=pkb,
    )

    return ctx


def custom_sql_var_constructor(snowflake_params, input_params):

    try:
        sql_var_dict = {
            'long_env': snowflake_params.get('long_env'),
            'short_env': snowflake_params.get('short_env'),
            'report': input_params.get('report'),
            'partition_date': input_params.get('partition_date'),
            'etl_run_ts': datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        }
    except Exception as e:
        raise Exception("ERROR: Could not construct SQL variable dict - %s" % e)

    return sql_var_dict


# Function to execute individual lines
def execute_query(connection, query):

    try:
        connection.cursor().execute(query)
        connection.cursor.close()
    except Exception as e:
        raise Exception("ERROR: Could not execute SQL query in Snowflake - " % e)

    return None


# Function to read and execute entire files
def execute_sql_file_stream(connection, snowflake_params, file, input_params):

    stream_execute_file = "execute.sql"
    print('Executing stream from SQL file "%s"' % file)
    sql_vars = custom_sql_var_constructor(snowflake_params, input_params)
    file_name = None
    print('Variables being passed through to sql - %s' % sql_vars)

    try:
        with open(file, 'r', encoding='utf-8') as rf:
            # This replaces the variables in the sql files with values
            with open(stream_execute_file, 'w', encoding='utf-8') as wf:
                for line in rf:
                    wf.write(line.format(**sql_vars))
            with open(stream_execute_file, 'r', encoding='utf-8') as f:
                for cur in connection.execute_stream(f):
                    for ret in cur:
                        print(ret[0])
            os.remove(stream_execute_file)
        file_name = sql_vars.get('partition_date') + '_' + sql_vars.get('etl_run_ts') + '.csv.gz'
    except Exception as e:
        raise Exception("ERROR: Could not execute SQL file stream in Snowflake - " % e)

    return file_name
