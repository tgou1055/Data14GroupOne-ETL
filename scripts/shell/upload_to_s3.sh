#!/bin/bash

# Exit script on any error
set -e

# Parameters
SCRIPTS_BUCKET=$1

# zip and upload lambda if updates, and output version to varName
update_lambda() {
  local lambda_filename=$1
  local varName=$2
  local version_id

#  if git diff --quiet HEAD~1 "scripts/lambda/${lambda_filename}"; then
#    echo "scripts/lambda/${lambda_filename} is up-to-date, skipping upload."
#  else
  echo "zipping and uploading scripts/lambda/${lambda_filename} to S3..."
  LAMBDA_DIR=$(mktemp -d)
  cp "scripts/lambda/${lambda_filename}" "$LAMBDA_DIR"
  cd "$LAMBDA_DIR"
  zip "${lambda_filename%.py}.zip" "${lambda_filename}"
  cd -
  aws s3 cp "${LAMBDA_DIR}/${lambda_filename%.py}.zip" "s3://${SCRIPTS_BUCKET}/lambda/${lambda_filename%.py}.zip"
  rm -r "$LAMBDA_DIR"
#  fi
  version_id=$(aws s3api list-object-versions --bucket "$SCRIPTS_BUCKET" --prefix "lambda/${lambda_filename%.py}.zip" --query 'Versions[?IsLatest].VersionId' --output text)
  echo "$varName=$version_id" >> $GITHUB_ENV
}

# Upload CloudFormation templates to S3
echo "Uploading CloudFormation templates to S3..."
aws s3 sync cloudformation/ s3://${SCRIPTS_BUCKET}/cloudformation/

# upload gluejob scripts
echo "Uploading Glue scripts to S3..."
aws s3 sync scripts/gluejob/ s3://${SCRIPTS_BUCKET}/gluejob/

# update lambda
update_lambda raw2staging.py LAMBDA_RAW2STAGING_VERSION

echo "All files uploaded successfully."

