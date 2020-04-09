import logging
import os
from boto3 import client
import actions, parameters

FORECAST_CLI = client('forecast')
LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)
ARN = 'arn:aws:forecast:us-east-1:{account}:dataset-import-job/{name}/{name}_{date}'


def lambda_handler(event, context):
    params = parameters.get_params(
        bucket_name=event['bucket'], key_name=os.environ['PARAMS_FILE']
    )
    status = None
    event['DatasetImportJobArn'] = ARN.format(
        account=event['AccountID'],
        date=event['currentDate'],
        name=params['Datasets'][0]['DatasetName']
    )
    try:
        status = FORECAST_CLI.describe_dataset_import_job(
            DatasetImportJobArn=event['DatasetImportJobArn']
        )

    except FORECAST_CLI.exceptions.ResourceNotFoundException:
        LOGGER.info(
            'Dataset import job not found! Will follow to create new job.'
        )

        FORECAST_CLI.create_dataset_import_job(
            DatasetImportJobName='{name}_{date}'.format(
                name=params['Datasets'][0]['DatasetName'],
                date=event['currentDate']
            ),
            DatasetArn=event['DatasetArn'],
            DataSource={
                'S3Config':
                    {
                        'Path':
                            's3://{bucket}/train/'.format(
                                bucket=event['bucket']
                            ),
                        'RoleArn':
                            os.environ['FORECAST_ROLE']
                    }
            },
            TimestampFormat=params['TimestampFormat']
        )
        status = FORECAST_CLI.describe_dataset_import_job(
            DatasetImportJobArn=event['DatasetImportJobArn']
        )

    actions.take_action(status['Status'])
    return event
