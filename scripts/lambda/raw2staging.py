import boto3
import os
# hello
s3 = boto3.client('s3')

def lambda_handler(event, context):
    source_bucket = os.environ['SOURCE_BUCKET']
    destination_bucket = os.environ['DESTINATION_BUCKET']

    # List all objects in the source bucket
    response = s3.list_objects_v2(Bucket=source_bucket)

    # Loop through each object in the source bucket
    if 'Contents' in response:
        for obj in response['Contents']:
            key = obj['Key']
            copy_source = {'Bucket': source_bucket, 'Key': key}
            dest_key = f"{os.path.splitext(os.path.basename(key))[0]}/{os.path.basename(key)}"
            
            s3.copy_object(CopySource=copy_source, Bucket=destination_bucket, Key=dest_key)
            print(f"Copied {key} to {dest_key} in bucket {destination_bucket}")

    return {
        'statusCode': 200,
        'body': 'Files copied successfully'
    }
