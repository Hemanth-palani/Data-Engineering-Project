import json
import boto3
import urllib.parse
import uuid

# Create Step Functions client
sfn = boto3.client('stepfunctions')

def lambda_handler(event, context):
    # Get bucket name and file key
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'])

    print(f"New file uploaded!")
    print(f"Bucket: {bucket}")
    print(f"File: {key}")

    # Filter only bronze folder
    if key.startswith("input/"):
        print("Raw data detected - triggering Step Function")

        response = sfn.start_execution(
            stateMachineArn="arn:aws:states:us-east-1:467091806023:stateMachine:netflix_project",
            name=str(uuid.uuid4()),  # unique execution name
            input=json.dumps({
                "bucket": bucket,
                "key": key
            })
        )

        print("Execution started:", response["executionArn"])

    return {
        'statusCode': 200,
        'body': json.dumps('Success')
    }