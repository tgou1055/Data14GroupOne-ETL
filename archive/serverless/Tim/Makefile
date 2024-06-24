ENVIRONMENT=dev

deploy:
	sls deploy --stage ${ENVIRONMENT} --verbose

package:
	sls package --stage ${ENVIRONMENT}

test:
	serverless invoke --function dataPipeline --stage ${ENVIRONMENT} --path event/data_pipeline.json

remove:
# The S3 bucket created by serverless.yml needs to be emptyed before run the following remove command.
	sls remove --stage ${ENVIRONMENT}