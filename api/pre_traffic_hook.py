import boto3
from botocore.exceptions import ClientError
import json
import os
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

sm = boto3.client('sagemaker')
cd = boto3.client('codedeploy')

def lambda_handler(event, context):
    logger.debug('event %s', json.dumps(event))
    endpoint_name = os.environ['ENDPOINT_NAME']
    logger.info('pre traffic for endpoint %s', endpoint_name)

    error_message = None
    try:
        # Update endpoint to enable data capture
        endpoint = sm.describe_endpoint(EndpointName=endpoint_name)
        status = endpoint['EndpointStatus']
        if status != 'InService':
            error_message = 'SageMaker endpoint: {} status: {} not InService'.format(
                endpoint_name, status)
        # TODO: Add checks to make sure endpoint data capture is enabled and correct uri
        # Add any aditional invocation checks here
    except ClientError as e:
        error_message = e.response['Error']['Message']
        logger.error('Error checking endpoint %s', error_message)

    try:
        if error_message != None:
            logger.info('put codepipeline failed: %s', error_message)
            response = cd.put_lifecycle_event_hook_execution_status(
                deploymentId=event['DeploymentId'],
                lifecycleEventHookExecutionId=event['LifecycleEventHookExecutionId'],
                status='Failed'
            )
            return {
                "statusCode": 400,
                "message": error_message
            }
        else:
            logger.info('put codepipeline success')
            response = cd.put_lifecycle_event_hook_execution_status(
                deploymentId=event['DeploymentId'],
                lifecycleEventHookExecutionId=event['LifecycleEventHookExecutionId'],
                status='Succeeded'
            )
            return {
                "statusCode": 200,
            }    
    except ClientError as e:
        # Error attempting to update the cloud formation
        logger.error('Unexpected codepipeline error')
        logger.error(e)
        return {
            "statusCode": 500,
            "message": e.response['Error']['Message']            
        }