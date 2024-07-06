import boto3
import os

s3 = boto3.client('s3')

def lambda_handler(event, context):
    source_bucket = 'data14group1-raw'
    destination_bucket = 'data14group1-staging'
    
    for record in event['Records']:
        key = record['s3']['object']['key']
        copy_source = {'Bucket': source_bucket, 'Key': key}
        dest_key = f"orders/{os.path.basename(key)}"
        
        s3.copy_object(CopySource=copy_source, Bucket=destination_bucket, Key=dest_key)
        print(f"Copied {key} to {dest_key} in bucket {destination_bucket}")

    return {
        'statusCode': 200,
        'body': 'Files copied successfully'
    }