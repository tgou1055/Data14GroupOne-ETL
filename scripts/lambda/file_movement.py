import boto3
import os
import gzip
import shutil

s3 = boto3.resource('s3')
s3_client = boto3.client('s3')

def lambda_handler(event, context):
    print(event)
    records = event['Records']
    source_bucket = os.environ['SOURCE_BUCKET']
    destination_bucket = os.environ['DESTINATION_BUCKET']
    
    for record in records:
        key = record['s3']['object']['key']
        copy_source = {'Bucket': source_bucket, 'Key': key}
        
        filename = key.split('/')[-1]
        prefix = filename.split('.')[0]

        # if gz file, first gunzip, other file types will be just copied
        if key.endswith('.gz'):
            #first download to the tmp folder
            local_gz_file = f'/tmp/{filename}'
            s3_client.download_file(source_bucket, key, local_gz_file)
            
            #unzip
            local_csv_file = local_gz_file[:-3]
            with gzip.open(local_gz_file, 'rb') as f_in:
                with open(local_csv_file, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            
            # upload unzip file
            dest_key = f'{prefix}/{local_csv_file.split("/")[-1]}'
            s3_client.upload_file(local_csv_file, destination_bucket, dest_key)

        else:
            dest_key = f'{prefix}/{filename}' 
            s3.meta.client.copy(copy_source, destination_bucket, dest_key)
        
        
