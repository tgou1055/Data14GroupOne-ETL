#!/bin/bash

# Exit script on any error
set -e

# Parameters
SCRIPTS_BUCKET=$1

# Upload Glue Job to S3
echo "Uploading Glue Job scripts to S3..."
aws s3 sync scripts/gluejob/ s3://${SCRIPTS_BUCKET}/gluejob/

# Upload Lambda function to S3
echo "Uploading Lambda function to S3..."
aws s3 cp scripts/lambda/raw2staging.py s3://${SCRIPTS_BUCKET}/lambda/raw2staging.py

# Upload Step Function definition to S3
echo "Uploading Step Function definition to S3..."
aws s3 cp statemachines/dataLakePipeline.json s3://${SCRIPTS_BUCKET}/statemachines/dataLakePipeline.json

# Upload CloudFormation templates to S3
echo "Uploading CloudFormation templates to S3..."
aws s3 sync cloudformation/ s3://${SCRIPTS_BUCKET}/cloudformation/

echo "All files uploaded successfully."
