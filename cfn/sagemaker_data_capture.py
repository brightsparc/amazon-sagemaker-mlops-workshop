import boto3
import botocore
import logging

from crhelper import CfnResource
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)
sm = boto3.client('sagemaker')

# cfnhelper makes it easier to implement a CloudFormation custom resource
helper = CfnResource()

# CFN Handlers

def lambda_handler(event, context):
    import json
    logger.debug(json.dumps(event))
    helper(event, context)


@helper.create
def create_handler(event, context):
    """
    Called when CloudFormation custom resource sends the create event
    """
    update_endpoint(event)


@helper.delete
def delete_handler(event, context):
    """
    Called when CloudFormation custom resource sends the delete event
    """
    delete_endpoint_config(event)

@helper.poll_create
def poll_create(event, context):
    """
    Return true if the resource has been created and false otherwise so
    CloudFormation polls again.
    """
    endpoint_name = get_endpoint_name(event)
    logger.info('Polling for update of endpoint: %s', endpoint_name)
    return is_endpoint_ready(endpoint_name)

@helper.update
def noop():
    """
    Not currently implemented but crhelper will throw an error if it isn't added
    """
    pass

def get_endpoint_name(event):
    return event['ResourceProperties']['EndpointName']

def is_endpoint_ready(endpoint_name):
    is_ready = False

    endpoint = sm.describe_endpoint(EndpointName=endpoint_name)
    status = endpoint['EndpointStatus']

    if status == 'InService':
        logger.info('Endpoint (%s) is ready', endpoint_name)
        is_ready = True
    elif status == 'Updating':
        logger.info('Endpoint (%s) still updating, waiting and polling again...', endpoint_name)
    else:
        raise Exception('Endpoint ({}) has unexpected status: {}'.format(endpoint_name, status))

    return is_ready

def update_endpoint(event):
    props = event['ResourceProperties']

    endpoint_name = get_endpoint_name(event)

    # Get the endpoint validate in service
    endpoint = sm.describe_endpoint(EndpointName=endpoint_name)
    status = endpoint['EndpointStatus']
    if status != 'InService':
        raise Exception('Endpoint ({}) has unexpected status: {}'.format(endpoint_name, status))

    # Get the current endpoint config
    endpoint_config_name = endpoint['EndpointConfigName']
    endpoint_config = sm.describe_endpoint_config(EndpointConfigName=endpoint_config_name)

    # Get data capture config as dict
    data_capture_config_dict = {
        'EnableCapture': True,
        'DestinationS3Uri': props['DataCaptureUri'],
        'InitialSamplingPercentage': int(props.get('InitialSamplingPercentage', 100)),
        'CaptureOptions': [
            {'CaptureMode': 'Input'}, 
            {'CaptureMode': 'Output'}
        ],
        'CaptureContentTypeHeader': {
            'CsvContentTypes': ['text/csv'],
            'JsonContentTypes': ['application/json']
        }
    }

    new_config_name = props['EndpointConfigName']

    # Get new config 
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
    response = sm.create_endpoint_config(**request)
    logger.info('Create endpoint config: %s', new_config_name)
    
    # Update endpoint to point to new config
    response = sm.update_endpoint(
        EndpointName=endpoint_name, EndpointConfigName=new_config_name
    )
    logger.info('Update endpoint: %s', endpoint_name)

    # Leave the old endpoint config to be cleaned up by cloud formation
    return new_config_name

def delete_endpoint_config(event):
    # Delete the newly created endpoint config
    new_config_name = event['ResourceProperties']['EndpointConfigName']
    logger.info('Deleting endpoint config: %s', new_config_name)
    try:
        sm.delete_endpoint_config(EndpointConfigName=new_config_name)
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFound':
            logger.info('Resource not found, nothing to delete')
        else:
            logger.error('Unexpected error while trying to delete endpoint config')
            raise e