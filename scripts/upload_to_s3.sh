#!/bin/bash

# Exit script on any error
set -e

# Parameters
SCRIPTS_BUCKET=$1

# Create a temporary directory for the Lambda function
TEMP_DIR=$(mktemp -d)
cp scripts/lambda/raw2staging.py ${TEMP_DIR}

# Zip the Lambda function
echo "Zipping Lambda function..."
cd ${TEMP_DIR}
zip raw2staging.zip raw2staging.py
cd -

# Upload Glue Job to S3
echo "Uploading Glue Job scripts to S3..."
aws s3 sync scripts/gluejob/ s3://${SCRIPTS_BUCKET}/gluejob/

# Upload zipped Lambda function to S3
echo "Uploading zipped Lambda function to S3..."
aws s3 cp ${TEMP_DIR}/raw2staging.zip s3://${SCRIPTS_BUCKET}/lambda/raw2staging.zip

# Upload Step Function definition to S3
echo "Uploading Step Function definition to S3..."
aws s3 cp statemachines/dataLakePipeline.json s3://${SCRIPTS_BUCKET}/statemachines/dataLakePipeline.json

# Upload CloudFormation templates to S3
echo "Uploading CloudFormation templates to S3..."
aws s3 sync cloudformation/ s3://${SCRIPTS_BUCKET}/cloudformation/

echo "All files uploaded successfully."
