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
    training_job_name = os.environ['TRAINING_JOB_NAME']
    print('function: {}:{} endpoint: {} event: {}'.format(
        function_name, function_version, endpoint_name, json.dumps(event)))

    error_message = None
    try:
        response = sm.describe_endpoint(EndpointName=endpoint_name)
        print('sagemaker describe_endpoint', response)
        status = response['EndpointStatus']
        if status != 'InService':
            error_message = 'Stagemaker endpoint status: {} not InService'.format(status)
        # Get validation location from training job
        response = sm.describe_training_job(TrainingJobName=training_job_name)
        print('sagemaker describe_training_job', response)
        val_uri = [r['DataSource']['S3DataSource']['S3Uri'] for r in
                    response['InputDataConfig'] if r['ChannelName'] == 'validation']
        if val_uri:
            print('found validation: {}'.format(val_uri[0]))
            # TODO: Download dataset
            # TODO: Invoke endpoint with validation set to get baseline dataset
            # TODO: Create baseline processing job from dataset
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
                

