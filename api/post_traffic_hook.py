import boto3
from botocore.exceptions import ClientError
import json
import os

cd = boto3.client('codedeploy')
lb = boto3.client('lambda')
sm = boto3.client('sagemaker')

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

    # Implement post traffic handler
    # See: https://awslabs.github.io/serverless-application-model/safe_lambda_deployments.html

    # Print the event
    function_name = os.environ['FUNCTION_NAME']
    function_version = os.environ['FUNCTION_VERSION']
    print('function: {}:{} event: {}'.format(function_name, function_version, json.dumps(event)))

    try:
        # Get the endpoint from previous live lambda
        response = lb.get_function(FunctionName=function_name, Qualifier='live')
        print('lambda get_function: {}'.format(function_name), response)
        live_version = int(response['Configuration']['Version'])
        env_vars = response['Configuration']['Environment']['Variables']
        live_endpoint_name = env_vars['ENDPOINT_NAME']

        # Log if this is already live
        if function_version == live_version:
            print('function {}:{} is already live'.format(function_name, function_version))

        # Delete the monitoring schedule aligned with old endpoint so endpoint 
        # What permission is this?
        response = sm.delete_monitoring_schedule(
            MonitoringScheduleName=live_endpoint_name
        )
        print('monitoring delete_monitoring_schedule: {}'.format(live_endpoint_name), response)
    except ClientError as e:
        print('error deleting monitoring schedule', e)

    response = cd.put_lifecycle_event_hook_execution_status(
        deploymentId=event['DeploymentId'],
        lifecycleEventHookExecutionId=event['LifecycleEventHookExecutionId'],
        status='Succeeded'
    )
    print('put_lifecycle_succeeded', response)
    return {
        "statusCode": 200
    }