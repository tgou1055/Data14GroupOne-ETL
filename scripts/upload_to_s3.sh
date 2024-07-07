#!/bin/bash

# Exit script on any error
set -e

# Parameters
SCRIPTS_BUCKET=$1

get_version_id() {
  local key=$1
  local varName=$2
  local version_id
  version_id=$(aws s3api list-object-versions --bucket "$SCRIPTS_BUCKET" --prefix $key --query 'Versions[?IsLatest].VersionId' --output text)
  echo "$varName=$version_id"
  echo "$varName=$version_id" >> $GITHUB_ENV
}

# upload gluejob scripts
aws s3 sync scripts/gluejob/ s3://${SCRIPTS_BUCKET}/gluejob/

# upload lambda to s3
#if git diff --quiet HEAD~1 scripts/lambda/raw2staging.py; then
#  echo "scripts/lambda/raw2staging.py is up-to-date, skipping upload."
#else
#  echo "Updates found in scripts/lambda/raw2staging.py, zipping and uploading to S3..."
#  LAMBDA_DIR=$(mktemp -d)
#  cp scripts/lambda/raw2staging.py "$LAMBDA_DIR"
#  cd "$LAMBDA_DIR"
#  zip raw2staging.zip raw2staging.py
#  cd -
#  aws s3 cp "${LAMBDA_DIR}/raw2staging.zip" "s3://${SCRIPTS_BUCKET}/lambda/raw2staging.zip"
#  rm -r "$LAMBDA_DIR"
#fi
#get_version_id "lambda/raw2staging.zip" LAMBDA_RAW2STAGING_VERSION

# Upload Step Function definition to S3
#echo "Uploading Step Function definition to S3..."
#aws s3 cp statemachines/dataLakePipeline.json s3://${SCRIPTS_BUCKET}/statemachines/dataLakePipeline.json

# Upload CloudFormation templates to S3
#echo "Uploading CloudFormation templates to S3..."
#aws s3 sync cloudformation/ s3://${SCRIPTS_BUCKET}/cloudformation/

echo "All files uploaded successfully."


