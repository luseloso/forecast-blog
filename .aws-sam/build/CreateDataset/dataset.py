import logging
import os
from boto3 import client
import actions, parameters

FORECAST_CLI = client('forecast')
ACCOUNTID = client('sts').get_caller_identity()['Account']
LOGGER = logging.getLogger()
ARN = 'arn:aws:forecast:us-east-1:{account}:dataset/{name}'

LOGGER.setLevel(logging.INFO)


def lambda_handler(event, context):
    params = parameters.get_params(
        bucket_name=event['bucket'], key_name=os.environ['PARAMS_FILE']
    )['Datasets']
    status = None
    event['DatasetArn'] = ARN.format(account=ACCOUNTID, name=params[0]['DatasetName'])
    event['AccountID'] = ACCOUNTID
    try:
        status = FORECAST_CLI.describe_dataset(DatasetArn=event['DatasetArn'])
    except FORECAST_CLI.exceptions.ResourceNotFoundException:
        LOGGER.info('Dataset not found! Will follow to create dataset.')
        for dataset in params:
            FORECAST_CLI.create_dataset(**dataset)
        status = FORECAST_CLI.describe_dataset(DatasetArn=event['DatasetArn'])

    actions.take_action(status['Status'])
    return event
