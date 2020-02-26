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
    helper(event, context)


@helper.create
def create_handler(event, context):
    """
    Called when CloudFormation custom resource sends the create event
    """
    return create_processing_job(event)

@helper.delete
def delete_handler(event, context):
    """
    Called when CloudFormation custom resource sends the delete event
    """
    processing_job_name = get_processing_job_name(event)
    delete_processing_job(processing_job_name)

@helper.poll_create
@helper.poll_update
def poll_create(event, context):
    """
    Return true if the resource has been created and false otherwise so
    CloudFormation polls again.
    """
    processing_job_name = get_processing_job_name(event)
    logger.info('Polling for creation of processing job: %s', processing_job_name)
    return is_processing_job_ready(processing_job_name)

@helper.update
def update_handler(event, context):
    """
    Not currently implemented but crhelper will throw an error if it isn't added
    """
    pass

# Helper Functions

def get_model_monitor_container_uri(region):
    container_uri_format = '{0}.dkr.ecr.{1}.amazonaws.com/sagemaker-model-monitor-analyzer'

    regions_to_accounts = {
        'eu-north-1': '895015795356',
        'me-south-1': '607024016150',
        'ap-south-1': '126357580389',
        'us-east-2': '680080141114',
        'us-east-2': '777275614652',
        'eu-west-1': '468650794304',
        'eu-central-1': '048819808253',
        'sa-east-1': '539772159869',
        'ap-east-1': '001633400207',
        'us-east-1': '156813124566',
        'ap-northeast-2': '709848358524',
        'eu-west-2': '749857270468',
        'ap-northeast-1': '574779866223',
        'us-west-2': '159807026194',
        'us-west-1': '890145073186',
        'ap-southeast-1': '245545462676',
        'ap-southeast-2': '563025443158',
        'ca-central-1': '536280801234'
    }

    container_uri = container_uri_format.format(regions_to_accounts[region], region)
    return container_uri

def get_processing_job_name(event):
    return event['ResourceProperties']['ProcessingJobName']

def is_processing_job_ready(processing_job_name):
    is_ready = False

    processing_job = sm.describe_processing_job(ProcessingJobName=processing_job_name)
    status = processing_job['ProcessingJobStatus']

    if status == 'Completed':
        logger.info('Processing Job (%s) is complete', processing_job_name)
        is_ready = True
    elif status == 'InProgress':
        logger.info('Processing Job (%s) still in progress, waiting and polling again...', processing_job_name)
    else:
        raise Exception('Processing Job ({}) has unexpected status: {}'.format(processing_job_name, status))

    return is_ready

def create_processing_job(event):
    props = event['ResourceProperties']

    processing_job_name = get_processing_job_name(event)

    # TODO: Allow specifying format in future
    dataset_format = { 'csv': {'header': True, 'output_columns_position': 'START'}}

    request = {
        "ProcessingInputs": [
            {
                "InputName": "baseline_dataset_input",
                "S3Input": {
                    "S3Uri": props['BaselineInputUri'],
                    "LocalPath": "/opt/ml/processing/input/baseline_dataset_input",
                    "S3DataType": "S3Prefix",
                    "S3InputMode": "File",
                    "S3DataDistributionType": "FullyReplicated",
                    "S3CompressionType": "None"
                }
            }
        ],
        "ProcessingOutputConfig": {
            "Outputs": [
                {
                    "OutputName": "monitoring_output",
                    "S3Output": {
                        "S3Uri": props['BaselineReportsUri'],
                        "LocalPath": "/opt/ml/processing/output",
                        "S3UploadMode": "EndOfJob"
                    }
                }
            ]
        },
        "ProcessingJobName": processing_job_name,
        "ProcessingResources": {
            "ClusterConfig": {
                "InstanceCount": 1,
                "InstanceType": props.get("InstanceType", "ml.t3.medium"),
                "VolumeSizeInGB": 20,
            }
        },
        "StoppingCondition": {
            "MaxRuntimeInSeconds": int(props.get("MaxRuntimeInSeconds", 300))
        },
        "AppSpecification": {
            "ImageUri": props.get("ImageURI", get_model_monitor_container_uri(helper._region))
        },
        "Environment": {
            "dataset_format": json.dumps(dataset_format),
            "dataset_source": "/opt/ml/processing/input/baseline_dataset_input",
            "output_path": "/opt/ml/processing/output",
            "publish_cloudwatch_metrics": props.get("PublishCloudwatchMetrics", "Disabled")
        },
        "RoleArn": props["PassRoleArn"],
    }

    logger.info('Creating processing job with name: %s', processing_job_name)

    response = sm.create_processing_job(**request)

    # Update Arn
    helper.Data['Arn'] = response["ProcessingJobArn"]
    return helper.Data['Arn']

def delete_processing_job(processing_job_name):
    logger.info('Deleting processing job: %s', processing_job_name)
    try:
        sm.delete_processing_job(MonitoringScheduleName=processing_job_name)
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFound':
            logger.info('Resource not found, nothing to delete')
        else:
            logger.error('Unexpected error while trying to delete monitoring processing job')
            raise e