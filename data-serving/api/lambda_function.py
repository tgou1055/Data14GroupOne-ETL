import json
import boto3
import os

runtime = boto3.client('runtime.sagemaker')

def lambda_handler(event, context):
    input_data = event['data']

    try:
        response = runtime.invoke_endpoint(
            EndpointName=os.environ['ENDPOINT_NAME'],
            ContentType='text/csv',
            Body=input_data
        )
        result = int(float(response['Body'].read().decode('ascii')))
        return {
            'statusCode': 200,
            'body': json.dumps({
                'prediction': result
            })
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Error getting prediction from sagemaker',
                'error': str(e)
            })
        }
