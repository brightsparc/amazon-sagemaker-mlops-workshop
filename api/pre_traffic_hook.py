import boto3
from botocore.exceptions import ClientError
import json
import os
import random
import time

sm = boto3.client('sagemaker')
cd = boto3.client('codedeploy')

def unique_name_from_base(base, max_length=63):
    """
    Args:
        base:
        max_length:
    """
    unique = "%04x" % random.randrange(16 ** 4)  # 4-digit hex
    ts = str(int(time.time()))
    available_length = max_length - 2 - len(ts) - len(unique)
    trimmed = base[:available_length]
    return "{}-{}-{}".format(trimmed, ts, unique)

def enable_data_capture(endpoint_name, data_capture_uri):
    # Get the endpoint validate in service
    endpoint = sm.describe_endpoint(EndpointName=endpoint_name)
    if endpoint['EndpointStatus'] != 'InService':
        return None

    # Get the current endpoint config
    endpoint_config_name = endpoint['EndpointConfigName']
    endpoint_config = sm.describe_endpoint_config(EndpointConfigName=endpoint_config_name)

    # Get data capture config as dict
    data_capture_config_dict = {
        'EnableCapture': True,
        'InitialSamplingPercentage': 100,
        'DestinationS3Uri': data_capture_uri,
        'CaptureOptions': [
            {'CaptureMode': 'Input'}, {'CaptureMode': 'Output'}
        ],
        'CaptureContentTypeHeader': {
            'CsvContentTypes': ['text/csv'],
            'JsonContentTypes': ['application/json']
        }
    }

    # Get new config name from endpoint_config
    new_config_name = unique_name_from_base(endpoint_config_name)

    request = {
        "EndpointConfigName": new_config_name,
        "ProductionVariants": endpoint_config["ProductionVariants"],
        "DataCaptureConfig": data_capture_config_dict,
        "Tags": [] # Don't copy aws:* tags from orignial
    }

    # Copy KmsKeyId if provided
    if endpoint_config.get("KmsKeyId") is not None:
        request["KmsKeyId"] = endpoint_config.get("KmsKeyId")

    # Create the endpoint config
    print('create endpoint config', request)
    response = sm.create_endpoint_config(**request)
    print('sagemaker create_endpoint_config', response)
    
    # Update endpoint to point to new config
    response = sm.update_endpoint(
        EndpointName=endpoint_name, EndpointConfigName=new_config_name
    )
    print('sagemaker update_endpoint', response)

    # Leave the old endpoint config to be cleaned up by cloud formation
    return new_config_name

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
    print('event', json.dumps(event))

    # Print the params
    endpoint_name = os.environ['ENDPOINT_NAME']
    data_capture_uri = os.environ['DATA_CAPTURE_URI']
    print('endpoint: {} data capture: {}'.format(endpoint_name, data_capture_uri))

    error_message = None
    try:
        # Update endpoint to enable data capture
        endpoint_config_name = enable_data_capture(endpoint_name, data_capture_uri)
        if endpoint_config_name == None:
            error_message = 'Stagemaker endpoint: {} not InService'.format(endpoint_name)
    except ClientError as e:
        print('sagemaker error', e)
        error_message = e.response['Error']['Message']

    try:
        if error_message:
            response = cd.put_lifecycle_event_hook_execution_status(
                deploymentId=event['DeploymentId'],
                lifecycleEventHookExecutionId=event['LifecycleEventHookExecutionId'],
                status='Failed'
            )
            print('codepipeline put_lifecycle_failed', response)
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
            print('codepipeline put_lifecycle_succeeded', response)
            return {
                "statusCode": 200,
            }    
    except ClientError as e:
        # Error attempting to update the cloud formation
        print('codepipeline error', e)
        return {
            "statusCode": 500,
            "message": e.response['Error']['Message']            
        }