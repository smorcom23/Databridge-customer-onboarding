import sys
import os
import boto3
import time
from snowflake_data_events.gpi_data_extraction import gpi_data_extraction
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


def get_secret(secret_id):
    client = boto3.client('secretsmanager')
    response = client.get_secret_value(
        SecretId=secret_id
    )
    return response['SecretString']


def get_parameter(parameter_id):
    client = boto3.client('ssm')
    response = client.get_parameter(
        Name=parameter_id, WithDecryption=True
    )
    return response['Parameter']['Value']


def __init__():
    #print envars
    print("os.environ: ", os.environ)
    # Get event params and construct sql file path
    print ("-------- Task Start -------")
    start_time = time.time()
    container_params = {
        'report': os.environ.get('REPORT').lower(),
        'dataset': os.environ.get('DATASET').lower(),
        'partition_date': os.environ.get('PARTITION_DATE').lower()
    }
    print('Extract file parameters for container : %s' % str(container_params))

    sql_script = container_params.get('report') + '-' + container_params.get('dataset') + '-extract.sql'
    print('Extract script to execute: %s' % sql_script)

    object_key_prefix = container_params.get('report') + '/' + container_params.get('partition_date')

    output_filename = gpi_data_extraction(object_key_prefix, sql_script, container_params)

    print(output_filename + " has successfully been created.")

    if 'SNS_ARN' in os.environ:
        # Create an SNS client
        sns = boto3.client('sns')
        # Publish a simple message to the specified SNS topic
        response = sns.publish(
            TopicArn=os.environ.get('SNS_ARN').lower(),
            Message='Output Data set Created: %s' % output_filename
        )
        # Print out the response
        print('sns_publish_response: ', response)

    iata_copy = os.environ.get('IATA_COPY')

    if iata_copy == "True":
        source_bucket = ''
        env = os.environ.get('ENVIRONMENT')
        
        s3 = boto3.client('s3')
       
        response = s3.list_buckets()

        for bucket in response['Buckets']:
            splitbucket = bucket["Name"].split("-")
            if len(splitbucket) < 5 and "arc" not in splitbucket[0] and splitbucket[2] == ("%s" % env) and splitbucket[3] == "customerout":
                source_bucket = bucket.get("Name")
                print(f'Setting {bucket["Name"]} as Source Bucket')

        report = container_params.get('report')
        partition_date = container_params.get('partition_date')

        source_file = report + "/" + partition_date + "/" + output_filename
        
        print("----------------------")
        print(source_file)
        
        source_response = s3.get_object(
          Bucket=source_bucket,
          Key=source_file
        )
        # Get Value from Secrets Manager
        aws_access_key_id = get_secret('/gpi/%s/iata_landing_s3_access_key' % env)
        aws_secret_access_key = get_secret('/gpi/%s/iata_landing_s3_secret_key' % env)
        
        s3_path = get_parameter('/gpi/%s/iata_landing/s3_path' % env)
        s3_bucket = get_parameter('/gpi/%s/iata_landing/s3_bucket' % env)
            
        print('Destination S3 Bucket:' + s3_bucket)
        print('Destination S3 Key:' + s3_path)

        destination_path = s3_path + source_file
        print('Destination Path:'+destination_path)
        
        print('starting IATA client')
        destination_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key
        )
        
        print('Copying: '+output_filename+' from: ' + source_bucket)

        print('Uploading to IATA')
        destination_client.upload_fileobj(
          source_response['Body'],
          s3_bucket, 
          destination_path,
        )       
        
        elapsed_time = time.time() - start_time
        
        print("Task Completed in: " + time.strftime("%H:%M:%S", time.gmtime(elapsed_time)))

    else: 
        elapsed_time = time.time() - start_time
        print("No need to copy data to IATA")
        print("Task Completed in: " + time.strftime("%H:%M:%S", time.gmtime(elapsed_time)))
        print("-----------")

    return None


# Initialize the SQL execution
if __name__ == '__main__':
    __init__()
