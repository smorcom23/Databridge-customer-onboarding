#!/usr/bin/env python
import boto3
import ast
import os


# Function to access secrets manager in AWS
def get_secret(secret_id):

    client = boto3.client('secretsmanager', region_name='us-east-1')
    secret_value = client.get_secret_value(SecretId=secret_id)
    secret = secret_value['SecretString']
    return secret


def get_stored_params():

    # Setting up clients and getting container variables
    print('Reading in parameters from AWS pstore and secrets manager.')
    ssm = boto3.client('ssm')
    product = os.environ.get('PRODUCT').lower()
    environment = os.environ.get('ENVIRONMENT').lower()
    sf_params_list = []
    nexttoken = None

    while True:
        if nexttoken is not None:
            sf_params = ssm.get_parameters_by_path(Path='/%s/%s/snflk_config' % (product, environment), Recursive=True,
                                                   NextToken=nexttoken)
        else:
            sf_params = ssm.get_parameters_by_path(Path='/%s/%s/snflk_config' % (product, environment), Recursive=True)
        sf_params_list += sf_params['Parameters']
        if 'NextToken' not in sf_params:
            break
        nexttoken = sf_params['NextToken']

    snowflake_params = dict(zip([d['Name'] for d in sf_params_list], [d['Value'] for d in sf_params_list]))
    snowflake_conn_params = {
        'account_name': snowflake_params['/%s/%s/snflk_config/snflk_accountname' % (product, environment)],
        'private_key_passphrase': get_secret('/%s/%s/snowflake-private-key-passphrase' % (product, environment)),
        'private_key_value': get_secret('/%s/%s/snowflake-private-key' % (product, environment)),
        'long_env': environment.upper(),  # 'DEV', 'UAT', 'QA', 'PROD'
        'short_env': environment[0].upper()  # 'D', 'U', 'Q', 'P'
    }

    snowflake_conn_params['username'] = snowflake_params['/%s/%s/snflk_config/snflk_username' % (product, environment)].replace(
        '${SHORT_ENV}', snowflake_conn_params['short_env'])

    return snowflake_conn_params


# Function to parse an SNS event which contains details of an S3 event
def parse_sns_event(event, notify_event_type):

    event_output = None
    sns_event_details = ast.literal_eval(event.get("Records")[0].get("Sns").get("Message"))

    if notify_event_type == 's3':
        object_key = sns_event_details.get("Records")[0].get("S3").get("object").get("Key")

        event_output = {
            'object_key': object_key,
            'doc_type': object_key.split('/')[3]
        }

    return event_output
