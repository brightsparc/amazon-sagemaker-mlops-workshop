import boto3
from botocore.exceptions import ClientError
import json
import os
import random
import time

cd = boto3.client('codedeploy')
lb = boto3.client('lambda')
sm = boto3.client('sagemaker')

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

def create_monitoring_schedule(region, role, endpoint_name, baseline_results_uri, monitoring_reports_uri,
                               schedule_expression='cron(0 * ? * * *)',
                               publish_cloudwatch_metrics='Enabled',
                               instance_size='ml.m5.xlarge',
                               stop_condition=3600):
    monitoring_schedule_name = unique_name_from_base(endpoint_name)
    request = {
        'MonitoringScheduleName': monitoring_schedule_name,
        'MonitoringScheduleConfig': {
            'ScheduleConfig': {
                'ScheduleExpression': schedule_expression
            },
            'MonitoringJobDefinition': {
                'BaselineConfig': {
                    'ConstraintsResource': {
                        'S3Uri': baseline_results_uri + '/constraints.json'
                    },
                    'StatisticsResource': {
                        'S3Uri': baseline_results_uri + '/statistics.json'
                    }
                },
                'MonitoringInputs': [
                    {
                        'EndpointInput': {
                            'EndpointName': endpoint_name,
                            'LocalPath': '/opt/ml/processing/input/endpoint',
                            'S3InputMode': 'File',
                            'S3DataDistributionType': 'FullyReplicated'
                        }
                    },
                ],
                'MonitoringOutputConfig': {
                    'MonitoringOutputs': [
                        {
                            'S3Output': {
                                'S3Uri': monitoring_reports_uri,
                                'LocalPath': '/opt/ml/processing/output',
                                'S3UploadMode': 'Continuous'
                            }
                        },
                    ],
#                     'KmsKeyId': ''
                },
                'MonitoringResources': {
                    'ClusterConfig': {
                        'InstanceCount': 1,
                        'InstanceType': instance_size,
                        'VolumeSizeInGB': 20,
#                         'VolumeKmsKeyId': None 
                    }
                },
                'MonitoringAppSpecification': {
                    'ImageUri': get_model_monitor_container_uri(region),
#                     'RecordPreprocessorSourceUri': '',
#                     'PostAnalyticsProcessorSourceUri': ''
                },
                'StoppingCondition': {
                    'MaxRuntimeInSeconds': stop_condition
                },
                'Environment': {
                    'publish_cloudwatch_metrics': publish_cloudwatch_metrics
                },
#                 'NetworkConfig': {
#                     'EnableNetworkIsolation': True|False,
#                     'VpcConfig': {
#                         'SecurityGroupIds': [
#                             'string',
#                         ],
#                         'Subnets': [
#                             'string',
#                         ]
#                     }
#                 },
                'RoleArn': role
            }
        }
    }
    print(request)
    return sm.create_monitoring_schedule(**request)

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
    print('event', json.dumps(event))

    # Print params
    function_name = os.environ['FUNCTION_NAME']
    function_version = int(os.environ['FUNCTION_VERSION'])
    endpoint_name = os.environ['ENDPOINT_NAME']
    baseline_results_uri = os.environ['BASELINE_RESULTS_URI']
    print('function: {}:{} endpoint: {} baseline results: {}'.format(
        function_name, function_version, endpoint_name, baseline_results_uri))
    monitoring_reports_uri = os.environ['MONITORING_REPORTS_URI']
    monitoring_role_arn = os.environ['MONITORING_ROLE_ARN']
    print('monitoring reports: {} role: {}'.format(
        monitoring_reports_uri, monitoring_role_arn))

    try:
        # Get the endpoint from previous live lambda
        response = lb.get_function(FunctionName=function_name, Qualifier='live')
        print('lambda get_function: {}'.format(function_name), response)
        live_version = int(response['Configuration']['Version'])
        env_vars = response['Configuration']['Environment']['Variables']
        live_endpoint_name = env_vars['ENDPOINT_NAME']
        print('function {}:{} live:{}'.format(function_name, function_version, live_version))
        # Delete the monitoring schedule for live endpoint
        if function_version > live_version:
            response = sm.list_monitoring_schedules(EndpointName=live_endpoint_name)
            print('sagemaker list_monitoring_schedules: {}'.format(live_endpoint_name), response)
            if response['MonitoringScheduleSummaries']:
                monitoring_schedule_name = response['MonitoringScheduleSummaries'][0]['MonitoringScheduleName']
                response = sm.delete_monitoring_schedule(MonitoringScheduleName=monitoring_schedule_name)
                print('sagemaker delete_monitoring_schedule: {}'.format(monitoring_schedule_name), response)
                time.sleep(5)
    except ClientError as e:
        print('error deleting monitoring schedule', e)

    try:
        # Create a new monitoring schedule from baseline results
        region = boto3.Session().region_name
        response = create_monitoring_schedule(region, monitoring_role_arn, endpoint_name, 
            baseline_results_uri, monitoring_reports_uri)
        print('sagemaker create_monitoring_schedule: {}'.format(endpoint_name), response)
    except ClientError as e:
        print('error creating monitoring schedule', e)

    response = cd.put_lifecycle_event_hook_execution_status(
        deploymentId=event['DeploymentId'],
        lifecycleEventHookExecutionId=event['LifecycleEventHookExecutionId'],
        status='Succeeded'
    )
    print('codepipeline put_lifecycle_succeeded', response)
    return {
        "statusCode": 200
    }