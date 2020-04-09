import logging
from boto3 import client
import actions

FORECAST_CLI = client('forecast')
LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)


def lambda_handler(event, context):
    # Delete forecast export job
    try:
        FORECAST_CLI.delete_forecast_export_job(
            ForecastExportJobArn=event['ExportJobArn']
        )
        actions.take_action_delete(
            status=FORECAST_CLI.describe_forecast_export_job(
                ForecastExportJobArn=event['ExportJobArn']
            )['Status']
        )
    except (FORECAST_CLI.exceptions.ResourceNotFoundException, KeyError):
        LOGGER.info('Forecast export job not found. Passing.')

    # Delete forecast
    try:
        FORECAST_CLI.delete_forecast(ForecastArn=event['ForecastArn'])
        actions.take_action_delete(
            FORECAST_CLI.describe_forecast(ForecastArn=event['ForecastArn']
                                          )['Status']
        )
    except (FORECAST_CLI.exceptions.ResourceNotFoundException, KeyError):
        LOGGER.info('Forecast not found. Passing.')

    return event
