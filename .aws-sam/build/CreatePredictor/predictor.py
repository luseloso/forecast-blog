import os
import logging
from boto3 import client
import actions, parameters

S3_CLI = client('s3')
FORECAST_CLI = client('forecast')
LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)
ARN = 'arn:aws:forecast:us-east-1:{account}:predictor/{name}'


def lambda_handler(event, context):
    status = None
    params = parameters.get_params(
        bucket_name=event['bucket'], key_name=os.environ['PARAMS_FILE']
    )['Predictor']
    event['PredictorArn'] = ARN.format(
        account=event['AccountID'],
        date=event['currentDate'],
        name=params['PredictorName'],
    )
    try:
        status = FORECAST_CLI.describe_predictor(
            PredictorArn=event['PredictorArn']
        )

    except FORECAST_CLI.exceptions.ResourceNotFoundException:
        LOGGER.info('Predictor not found! Will follow to create new predictor.')
        if 'InputDataConfig' in params.keys():
            params['InputDataConfig']['DatasetGroupArn']=event['DatasetGroupArn']
        else:
            params['InputDataConfig']={'DatasetGroupArn': event['DatasetGroupArn']}
        FORECAST_CLI.create_predictor(**params)
        status = FORECAST_CLI.describe_predictor(
            PredictorArn=event["PredictorArn"]
        )
    actions.take_action(status['Status'])
    return event
