import json
import boto3

s3 = boto3.client('s3')

def lambda_handler(event, context):
    s3_bucket = event['s3_bucket']
    s3_key = event['s3_key']
    if not s3_bucket or not s3_key:
        return {
            'statusCode': 400,
            'body': json.dumps({
                'message': 's3_bucket and s3_key are required'
            })
        }

    try:
        # Get the object from S3
        s3_response = s3.get_object(Bucket=s3_bucket, Key=s3_key)
        file_content = s3_response['Body'].read().decode('utf-8')
        
        return {
            'statusCode': 200,
            'body': file_content
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Error reading file from S3',
                'error': str(e)
            })
        }