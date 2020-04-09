import logging
import os
from boto3 import client
import actions, parameters

S3_CLI = client('s3')
FORECAST_CLI = client('forecast')
LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)
ARN = 'arn:aws:forecast:us-east-1:{account}:dataset-group/{name}'


def lambda_handler(event, context):
    params = parameters.get_params(
        bucket_name=event['bucket'], key_name=os.environ['PARAMS_FILE']
    )['DatasetGroup']
    status = None
    event['DatasetGroupArn'] = ARN.format(
        account=event['AccountID'], name=params['DatasetGroupName']
    )
    try:
        status = FORECAST_CLI.describe_dataset_group(
            DatasetGroupArn=event['DatasetGroupArn']
        )

    except FORECAST_CLI.exceptions.ResourceNotFoundException:
        LOGGER.info(
            'Dataset Group not found! Will follow to create Dataset Group.'
        )
        FORECAST_CLI.create_dataset_group(
            **params, DatasetArns=[event['DatasetArn']]
        )
        status = FORECAST_CLI.describe_dataset_group(
            DatasetGroupArn=event['DatasetGroupArn']
        )

    actions.take_action(status['Status'])
    return event
