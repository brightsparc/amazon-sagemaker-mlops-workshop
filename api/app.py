import boto3
import json
import os
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

sm_runtime = boto3.client('sagemaker-runtime')

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

    # Ensure we have permissions
    # See: https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-resource-sagemaker-endpoint.html

    # Print the event
    logger.debug('event %s', json.dumps(event))
    endpoint_name = os.environ['ENDPOINT_NAME']
    logger.info('api for endpoint %s', endpoint_name)

    # Get posted body and content type
    content_type = event['headers'].get('Content-Type', 'text/csv')
    body = json.loads(event['body'])
    payload = body.get('data')
    header, lines = payload.split('\n', 1)
    logger.info('content type: %s csv header: %s lines: %d', content_type, header, len(lines))

    try:
        #  Split payload into multiple lines so that we capture individual records
        predictions = []
        for i, line in enumerate(lines):
            logger.debug('%d: %s', i, line)
            body = header + '\n' + line
            response = sm_runtime.invoke_endpoint(
                EndpointName=endpoint_name,
                Body=body,
                ContentType=content_type,
                Accept='application/json'
            )
            # Append predictions as bytes
            predictions += response['Body'].read()

        return {
            "statusCode": 200,
            "body": json.dumps({
                "endpoint_name": endpoint_name, 
                "predictions": predictions.decode('utf-8')
            }),
        }
    except ClientError as e:
        logger.error('Unexpected sagemaker error')
        logger.error(e)
        return {
            "statusCode": 500,
            "message": e.response['Error']['Message']
        }