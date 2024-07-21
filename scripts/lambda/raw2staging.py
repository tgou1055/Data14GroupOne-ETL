import boto3
import os
import gzip
import shutil
import logging

# test bucket versioning 2

s3 = boto3.resource('s3')
s3_client = boto3.client('s3')
logging.basicConfig(level=logging.WARNING)


def lambda_handler(event, context):
    source_bucket = os.environ['SOURCE_BUCKET']
    destination_bucket = os.environ['DESTINATION_BUCKET']
    objects = s3_client.list_objects_v2(Bucket=source_bucket)
    for obj in objects.get('Contents', []):
        key = obj['Key']
        copy_source = {'Bucket': source_bucket, 'Key': key}
        # if gz file, first gunzip, other file types will be just copied
        if key.endswith('.csv.gz'):
            print(f"unzipping {key}")
            # first download to the tmp folder
            local_gz_file = f'/tmp/{key.split("/")[-1]}'
            s3_client.download_file(source_bucket, key, local_gz_file)
            # unzip
            local_csv_file = local_gz_file[:-3]
            with gzip.open(local_gz_file, 'rb') as f_in:
                with open(local_csv_file, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            # upload unzip file
            fn = f'{local_csv_file.split("/")[-1]}'
            prefix = fn[:-4]
            dest_key = f"{prefix}/{fn}"
            s3_client.upload_file(local_csv_file, destination_bucket, dest_key)
        elif key.endswith('.csv'):
            print(f"copying {key}")
            fn = key.split("/")[-1]
            prefix = fn[:-4]
            dest_key = f"{prefix}/{fn}"
            s3.meta.client.copy(copy_source, destination_bucket, dest_key)
        else:
            logging.warning(f"raw2staging: File {key} does not end with .csv.gz or .csv and will not be processed.")
    return {"raw2staging": {
        'statusCode': 200,
        'body': 'raw2staging: ingestion complete.'
    }}
