import json
import boto3

stepfunctions = boto3.client("stepfunctions")

STATE_MACHINE_ARN = "arn:aws:states:region:account:stateMachine:enterprise-etl"

def lambda_handler(event, context):
    response = stepfunctions.start_execution(
        stateMachineArn=STATE_MACHINE_ARN,
        input=json.dumps(event)
    )
    return {
        "status": "STARTED",
        "executionArn": response["executionArn"]
    }
