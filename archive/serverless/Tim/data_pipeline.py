import json
import boto3
import os


def lambda_handler(event, context):
    glue_client = boto3.client('glue')
    s3_event = event['Records'][0]['s3']
    
    bucket_name = s3_event['bucket']['name']
    object_key = s3_event['object']['key']

    glue_job_name = 'my-glue-script'
    
    try:
        response = glue_client.start_job_run(
            JobName= glue_job_name,
            Arguments={
                '--S3_BUCKET': bucket_name,
                '--S3_KEY': object_key
            }
        )
        return {
            'statusCode': 200,
            'body': json.dumps('Glue job started successfully!')
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps('Error starting Glue job: ' + str(e))
        }
