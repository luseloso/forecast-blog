import os
from json import dumps
from boto3 import client

SNS = client('sns')


def lambda_handler(event, context):
    return SNS.publish(
        TopicArn=os.environ['SNS_TOPIC_ARN'], Message=dumps(event)
    )
