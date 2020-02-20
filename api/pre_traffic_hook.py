import boto3
from botocore.exceptions import ClientError
import json
import os

sm = boto3.client('sagemaker')
cd = boto3.client('codedeploy')

def lambda_handler(event, context):
    """Sample pure Lambda function

    Parameters
    ----------
    event: dict, required
        API Gateway Lambda Proxy Input Format

        Event doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html#api-gateway-simple-proxy-for-lambda-input-format

    context: object, required
        Lambda Context runtime methods and attributes

        Context doc: https://docs.aws.amazon.com/lambda/latest/dg/python-context-object.html

    Returns
    ------
    API Gateway Lambda Proxy Output Format: dict

        Return doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html
    """

    # Implement pre traffic handler
    # See: https://awslabs.github.io/serverless-application-model/safe_lambda_deployments.html

    # Print the event
    function_name = os.environ['FUNCTION_NAME']
    function_version = os.environ['FUNCTION_VERSION']
    endpoint_name = os.environ['ENDPOINT_NAME']
    print('function: {}:{} endpoint: {} event: {}'.format(
        function_name, function_version, endpoint_name, json.dumps(event)))

    error_message = None
    try:
        status = sm.describe_endpoint(EndpointName=endpoint_name)['EndpointStatus']
        if status != 'InService':
            error_message = 'Stagemaker endpoint status: {} not InService'.format(status)
        # TODO: Add other checks to invoke endpoint if required
    except ClientError as e:
        error_message = e.response['Error']['Message']

    try:
        if error_message:
            print('invoke endpoint error', error_message)
            response = cd.put_lifecycle_event_hook_execution_status(
                deploymentId=event['DeploymentId'],
                lifecycleEventHookExecutionId=event['LifecycleEventHookExecutionId'],
                status='Failed'
            )
            print('put_lifecycle_failed', response)
            return {
                "statusCode": 400,
                "message": error_message
            }
        else:
            response = cd.put_lifecycle_event_hook_execution_status(
                deploymentId=event['DeploymentId'],
                lifecycleEventHookExecutionId=event['LifecycleEventHookExecutionId'],
                status='Succeeded'
            )
            print('put_lifecycle_succeeded', response)
            return {
                "statusCode": 200,
            }    
    except ClientError as e:
        # Error attempting to update the cloud formation
        print('code deploy error', e)
        return {
            "statusCode": 500,
            "message": e.response['Error']['Message']            
        }
                

