#!/bin/bash
region=$(aws configure get region)
accountid=$(aws sts get-caller-identity --query Account --output text)

printf "\nCreating S3 bucket to upload Lambda functions zip files...\n"
if [ ${region} == 'us-east-1' ]
then
    aws s3api create-bucket --bucket imdb-ddb-os-lab-${region}-${accountid} --region ${region}
else
    aws s3api create-bucket --bucket imdb-ddb-os-lab-${region}-${accountid} --region ${region} --create-bucket-configuration LocationConstraint=${region}
fi

if [ $? != 0 ]; then exit 1; fi

printf "\nUploading Lambda functions zip files into S3 bucket...\n"
aws s3 cp ../serverless/lambda_wiring_function.zip s3://imdb-ddb-os-lab-${region}-${accountid}/
aws s3 cp ../serverless/lambda_ddb_update_function.zip s3://imdb-ddb-os-lab-${region}-${accountid}/
aws s3 cp ../serverless/lambda_ddb_streaming_function.zip s3://imdb-ddb-os-lab-${region}-${accountid}/
aws s3 cp ../datasource/ImdbTrimmed.txt s3://imdb-ddb-os-lab-${region}-${accountid}/
if [ $? != 0 ]; then exit 1; fi

printf "\nLambda functions setup complete!\n"