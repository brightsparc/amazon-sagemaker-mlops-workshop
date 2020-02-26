import boto3
import json
import os
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

sm_runtime = boto3.client('sagemaker-runtime')

def lambda_handler(event, context):
    logger.debug('event %s', json.dumps(event))
    endpoint_name = os.environ['ENDPOINT_NAME']
    logger.info('api for endpoint %s', endpoint_name)

    # Get posted body and content type
    content_type = event['headers'].get('Content-Type', 'text/csv')
    body = json.loads(event['body'])
    payload = body.get('data')
    logger.info('content type: %s size: %d', content_type, len(lines))

    try:
        # Invoke the endpoint with full multi-line payload
        response = sm_runtime.invoke_endpoint(
            EndpointName=endpoint_name,
            Body=payload,
            ContentType=content_type,
            Accept='application/json'
        )
        # TODO: Return predictions as JSON dictionary instead of CSV text
        predictions = response['Body'].read().decode('utf-8')
        return {
            "statusCode": 200,
            "headers": {"content-type": "application/json"},
            "body": json.dumps({
                "endpoint_name": endpoint_name, 
                "predictions": predictions
            }),
        }
    except ClientError as e:
        logger.error('Unexpected sagemaker error')
        logger.error(e)
        return {
            "statusCode": 500,
            "message": e.response['Error']['Message']
        }