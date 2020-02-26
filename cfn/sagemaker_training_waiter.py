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
@helper.delete
@helper.update
def create_handler(event, context):
    """
    Called when CloudFormation custom resource sends the delete event
    """
    pass

@helper.poll_create
@helper.poll_update
def poll_create(event, context):
    """
    Return true if the resource has been created and false otherwise so
    CloudFormation polls again.
    """
    training_job_name = get_training_job_name(event)
    logger.info('Polling for training job: %s', training_job_name)
    return is_training_job_ready(training_job_name)

# Helper Functions

def get_training_job_name(event):
    return event['ResourceProperties']['TrainingJobName']

def is_training_job_ready(training_job_name):
    response = sm.describe_training_job(TrainingJobName=training_job_name)
    status = response['TrainingJobStatus']

    if status == 'Completed':
        logger.info('Training Job (%s) is Completed', training_job_name)
        # Return additional info
        helper.Data['Arn'] = response['TrainingJobArn'] 
        return True
    elif status == 'InProgress':
        logger.info('Training job (%s) still in progress (%s), waiting and polling again...', 
            training_job_name, response['SecondaryStatus'])
    else:
        raise Exception('Training job ({}) has unexpected status: {}'.format(training_job_name, status))

    return False