import os
import logging
from boto3 import client
import actions, parameters

FORECAST_CLI = client('forecast')
CLOUDWATCH_CLI = client('cloudwatch')
LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)
ARN = 'arn:aws:forecast:us-east-1:{account}:forecast/{name}'
JOB_ARN = 'arn:aws:forecast:us-east-1:{account}:forecast-export-job/' \
          '{name}/{name}_{date}'


# Post training accuracy metrics from the previous step (predictor) to CloudWatch
def post_metric(metrics):
    # print(dumps(metrics))
    for metric in metrics['PredictorEvaluationResults']:
        CLOUDWATCH_CLI.put_metric_data(
            Namespace='FORECAST',
            MetricData=[
                {
                    'Dimensions':
                        [
                            {
                                'Name': 'Algorithm',
                                'Value': metric['AlgorithmArn']
                            }, {
                                'Name': 'Quantile',
                                'Value': str(quantile['Quantile'])
                            }
                        ],
                    'MetricName': 'WQL',
                    'Unit': 'None',
                    'Value': quantile['LossValue']
                } for quantile in metric['TestWindows'][0]['Metrics']
                ['WeightedQuantileLosses']
            ] + [
                {
                    'Dimensions':
                        [
                            {
                                'Name': 'Algorithm',
                                'Value': metric['AlgorithmArn']
                            }
                        ],
                    'MetricName': 'RMSE',
                    'Unit': 'None',
                    'Value': metric['TestWindows'][0]['Metrics']['RMSE']
                }
            ]
        )


def lambda_handler(event, context):
    params = parameters.get_params(
        bucket_name=event['bucket'], key_name=os.environ['PARAMS_FILE']
    )['Forecast']
    status = None
    event['ForecastArn'] = ARN.format(
        account=event['AccountID'], name=params['ForecastName']
    )
    event['ForecastExportJobArn'] = JOB_ARN.format(
        account=event['AccountID'],
        name=params['ForecastName'],
        date=event['currentDate']
    )

    # Creates Forecast and export Predictor metrics if Forecast does not exist yet.
    # Will throw an exception while the forecast is being created.
    try:
        actions.take_action(
            FORECAST_CLI.describe_forecast(ForecastArn=event['ForecastArn']
                                          )['Status']
        )
    except FORECAST_CLI.exceptions.ResourceNotFoundException:
        post_metric(
            FORECAST_CLI.get_accuracy_metrics(
                PredictorArn=event['PredictorArn']
            )
        )
        LOGGER.info('Forecast not found. Creating new forecast.')
        FORECAST_CLI.create_forecast(
            **params, PredictorArn=event['PredictorArn']
        )
        actions.take_action(
            FORECAST_CLI.describe_forecast(ForecastArn=event['ForecastArn']
                                          )['Status']
        )

    # Creates forecast export job if it does not exist yet. Will trhow an exception
    # while the forecast export job is being created.
    try:
        status = FORECAST_CLI.describe_forecast_export_job(
            ForecastExportJobArn=event['ForecastExportJobArn']
        )
    except FORECAST_CLI.exceptions.ResourceNotFoundException:
        LOGGER.info('Forecast export not found. Creating new export.')
        FORECAST_CLI.create_forecast_export_job(
            ForecastExportJobName='{name}_{date}'.format(
                name=params['ForecastName'], date=event['currentDate']
            ),
            ForecastArn=event['ForecastArn'],
            Destination={
                'S3Config':
                    {
                        'Path':
                            's3://{bucket}/tmp/'.format(bucket=event['bucket']),
                        'RoleArn':
                            os.environ['EXPORT_ROLE']
                    }
            }
        )
        status = FORECAST_CLI.describe_forecast_export_job(
            ForecastExportJobArn=event['ForecastExportJobArn']
        )

    actions.take_action(status['Status'])
    return event
