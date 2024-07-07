import boto3
import os
import gzip
import shutil

s3 = boto3.resource('s3')
s3_client = boto3.client('s3')

def lambda_handler(event, context):
    source_bucket = os.environ['SOURCE_BUCKET']
    destination_bucket = os.environ['DESTINATION_BUCKET']
    # List all objects in the source bucket
    objects = s3_client.list_objects_v2(Bucket=source_bucket)

    for obj in objects.get('Contents', []):
        key = obj['Key']
        copy_source = {'Bucket': source_bucket, 'Key': key}
        
        # if gz file, first gunzip, other file types will be just copied
        if key.endswith('.csv.gz'):
            #first download to the tmp folder
            local_gz_file = f'/tmp/{key.split("/")[-1]}'
            s3_client.download_file(source_bucket, key, local_gz_file)
            
            #unzip
            local_csv_file = local_gz_file[:-3]
            with gzip.open(local_gz_file, 'rb') as f_in:
                with open(local_csv_file, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            
            # upload unzip file
            dest_key = f'{local_csv_file.split("/")[-1]}'
            s3_client.upload_file(local_csv_file, destination_bucket, dest_key)

        elif key.endswith('.csv'):
            dest_key = f'{key.split("/")[-1]}' 
            s3.meta.client.copy(copy_source, destination_bucket, dest_key)
        else:
            print(f"raw2staging: {key} doesn't end with .csv.gz nor .csv")
        
        
