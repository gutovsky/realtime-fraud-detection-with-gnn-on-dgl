import logging
import boto3
from os import environ
import numpy as np
import json

logger = logging.getLogger()
logger.setLevel(logging.INFO)

inference = boto3.client('lambda')

INFERENCE_ARN = environ['INFERENCE_ARN']

def handler(event, context):
    logger.info(f'Event received {event}')
    inference_input_event = event['body']
    logger.info(f'Send event {inference_input_event} to inference.')

    inference_response = inference.invoke(FunctionName=INFERENCE_ARN,
                                        InvocationType='RequestResponse',
                                        Payload=inference_input_event)

    inference_result = inference_response["Payload"].read().decode()

    logger.info(f'Get result {inference_result} from inference.')
    return inference_result