import logging
from boto3 import client
import actions

FORECAST_CLI = client('forecast')
LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)


def lambda_handler(event, context):
    try:
        FORECAST_CLI.delete_predictor(PredictorArn=event['PredictorArn'])
        actions.take_action_delete(
            FORECAST_CLI.describe_predictor(PredictorArn=event['PredictorArn']
                                           )['Status']
        )

    except (FORECAST_CLI.exceptions.ResourceNotFoundException, KeyError):
        LOGGER.info('Predictor not found! Passing.')

    return event
