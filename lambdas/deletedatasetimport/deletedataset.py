import logging
from boto3 import client
import actions

FORECAST_CLI = client('forecast')
LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)


def lambda_handler(event, context):
    try:
        FORECAST_CLI.delete_dataset_import_job(
            DatasetImportJobArn=event['DatasetImportJobArn']
        )
        actions.take_action_delete(
            FORECAST_CLI.describe_dataset_import_job(
                DatasetImportJobArn=event['DatasetImportJobArn']
            )['Status']
        )

    except (FORECAST_CLI.exceptions.ResourceNotFoundException, KeyError):
        LOGGER.info('Import job not found! Passing.')

    return event
