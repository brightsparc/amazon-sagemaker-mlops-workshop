import argparse
import json
import os
import sys
import time
import boto3

def sagemaker_timestamp():
    """Return a timestamp with millisecond precision."""
    moment = time.time()
    moment_ms = repr(moment).split(".")[1][:3]
    return time.strftime("%Y-%m-%d-%H-%M-%S-{}".format(moment_ms), time.gmtime(moment))

def sagemaker_short_timestamp():
    """Return a timestamp that is relatively short in length"""
    return time.strftime("%y%m%d-%H%M")

def name_from_base(base, max_length=63, short=False):
    """Append a timestamp to the provided string.
    This function assures that the total length of the resulting string is
    not longer than the specified max length, trimming the input parameter if
    necessary.
    Args:
        base (str): String used as prefix to generate the unique name.
        max_length (int): Maximum length for the resulting string.
        short (bool): Whether or not to use a truncated timestamp.
    Returns:
        str: Input parameter with appended timestamp.
    """
    timestamp = sagemaker_short_timestamp() if short else sagemaker_timestamp()
    trimmed_base = base[: max_length - len(timestamp) - 1]
    return "{}-{}".format(trimmed_base, timestamp)
    
def get_training_request(model_name, job_id, role, image_uri, input_data, hyperparameters, output_uri):
    return {
        "TrainingJobName": name_from_base('mlops-{}'.format(job_id), short=True),
        "RoleArn": role,
        "AlgorithmSpecification": {
            "TrainingImage": image_uri,
            "TrainingInputMode": "File",
            "MetricDefinitions": [
                {'Name':'training:loss', 'Regex':'training loss: (.*?);'},
                {'Name':'train:accuracy', 'Regex':'training accuracy: (.*?)%;'},
                {'Name':'val:loss', 'Regex':'validation loss: (.*?);'},
                {'Name':'val:accuracy', 'Regex':'validation accuracy: (.*?)%;'},
                {'Name':'test:loss', 'Regex':'test loss: (.*?);'},
                {'Name':'test:accuracy', 'Regex':'test accuracy: (.*?)%;'}
            ]
        },
        "InputDataConfig": input_data,
        "HyperParameters": hyperparameters,
        "OutputDataConfig": {
            "S3OutputPath": output_uri
        },
        "ResourceConfig": {
            "InstanceCount": 1,
            "InstanceType": "ml.m4.xlarge",
            "VolumeSizeInGB": 30
        },
        "StoppingCondition": 360000,
        "ExperimentConfig": {   
            "ExperimentName": model_name,
            "TrialName": job_id,
            "TrialComponentDisplayName": 'Training'
        },
        "Tags": [] # This is required
    }

def get_experiment(model_name):
    return {
        "ExperimentName": model_name,
        "DisplayName": "Training",
        "Description": "Training for {}".format(model_name),
    }

def get_trial(model_name, job_id):
    return {
        "TrialName": job_id,
        "ExperimentName": model_name,
        "DisplayName": "Training",
        "Description": "Training for {}".format(model_name),
    }

def get_suggest_baseline(model_name, job_id, role, baseline_uri):
    return {
        "Parameters": {
            "ModelName": model_name,
            "TrainJobId": job_id,
            "MLOpsRoleArn": role,
            "BaselineInputUri": baseline_uri,
        }
    }

def get_dev_params(model_name, job_id, role, image_uri):
    return {
        "Parameters": {
            "ModelName": model_name,
            "TrainJobId": job_id,
            "MLOpsRoleArn": role,
            "ImageRepoUri": image_uri,
        }
    }

def get_prd_params(model_name, job_id, role, image_uri, 
                   metric_name='feature_baseline_drift_class_predictions', metric_threshold=0.4):
    dev_params = get_dev_params(model_name, job_id, role, image_uri)['Parameters']
    prod_params = {
        "ScheduleMetricName": metric_name, # alarm on class predictions drift
        "ScheduleMetricThreshold": str(metric_threshold) # Must serialize parameters as string
    }    
    return {
        "Parameters": dict(dev_params, **prod_params)
    }
    
def main(pipeline_name, model_name, role, data_bucket, ecr_dir, data_dir, output_dir):
    # Get pipeline execution id
    codepipeline = boto3.client('codepipeline')
    response = codepipeline.get_pipeline_state(name=pipeline_name)
    job_id = response['stageStates'][0]['latestExecution']['pipelineExecutionId']

    print('job id: {}'.format(job_id))
    output_uri = 's3://{0}/{1}'.format(data_bucket, model_name)
    baseline_uri = 's3://{0}/{1}/monitoring/baseline/mlops-{1}-pbl-{2}'.format(data_bucket, model_name, job_id)
    
    # Load the image uri and input data config
    with open(os.path.join(ecr_dir, 'imageDetail.json'), 'r') as f:
        image_uri = json.load(f)['ImageURI']
        print('image uri: {}'.format(image_uri))
        
    with open(os.path.join(data_dir, 'inputData.json'), 'r') as f:
        input_data = json.load(f)
        print('input data: {}'.format(input_data))
        
    hyperparameters = {}
    if os.path.exists(os.path.join(data_dir, 'hyperparameters.json')):
        with open(os.path.join(data_dir, 'hyperparameters.json'), 'r') as f:
            hyperparameters = json.load(f)    
            for i in hyperparameters:
                hyperparameters[i] = str(hyperparameters[i])
    
    # Create output directory
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)

    # Write experiment and trial config
    with open(os.path.join(output_dir, 'experiment.json'), 'w') as f:
        config = get_experiment(model_name)
        json.dump(config, f)
    with open(os.path.join(output_dir, 'trial.json'), 'w') as f:
        config = get_trial(model_name, job_id)
        json.dump(config, f)
                
    # Write the training request
    with open(os.path.join(output_dir, 'trainingjob.json'), 'w') as f:
        request = get_training_request(model_name, job_id, role, image_uri, 
                                       input_data, hyperparameters, output_uri)
        json.dump(request, f)

    # Write the baseline params for CFN
    with open(os.path.join(output_dir, 'suggest-baseline.json'), 'w') as f:
        params = get_suggest_baseline(model_name, job_id, role, baseline_uri)
        json.dump(params, f)        

    # Write the dev & prod params for CFN
    with open(os.path.join(output_dir, 'deploy-model-dev.json'), 'w') as f:
        params = get_dev_params(model_name, job_id, role, image_uri)
        json.dump(request, f)        
    with open(os.path.join(output_dir, 'template-model-prd.json'), 'w') as f:
        params = get_prd_params(model_name, job_id, role, image_uri)
        json.dump(params, f)        
        
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Load parameters')
    parser.add_argument('--pipeline-name')
    parser.add_argument('--model-name')
    parser.add_argument('--role')
    parser.add_argument('--data-bucket')
    parser.add_argument('--ecr-dir')
    parser.add_argument('--data-dir')
    parser.add_argument('--output-dir')
    args = parser.parse_args()
    print('args: {}'.format(args))
    main(args.pipeline_name, args.model_name, args.role, args.data_bucket, 
         args.ecr_dir, args.data_dir, args.output_dir)