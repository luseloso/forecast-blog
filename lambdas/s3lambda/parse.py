import os
from json import dumps
from datetime import datetime
from boto3 import client

STEP_FUNCTIONS_CLI = client('stepfunctions')


def lambda_handler(event, context):
    return dumps(
        STEP_FUNCTIONS_CLI.start_execution(
            stateMachineArn=os.environ['STEP_FUNCTIONS_ARN'],
            name=datetime.now().strftime("%Y_%m_%d_%H_%M_%S"),
            input=dumps(
                {
                    'bucket': event['Records'][0]['s3']['bucket']['name'],
                    'currentDate': datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
                }
            )
        ),
        default=str
    )
