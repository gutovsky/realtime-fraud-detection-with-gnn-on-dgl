import json
import boto3
import logging
from os import environ

logger = logging.getLogger()
logger.setLevel(logging.INFO)

STREAM_NAME = environ['STREAM_NAME']

def handler(event, context):
    logger.info(f"Received event: {event}")
    # Initialize the boto3 client for Kinesis Firehose
    firehose_client = boto3.client('firehose')
   
    # Process each record in the event
    for record in event['Records']:
        # Extract the body of the SQS message
        message_body = record['body']
        logger.info(f"Received message: {message_body}")

        # Transform the message into the format Firehose expects (if necessary)
        # This example assumes the message is a simple string that needs to be converted to bytes.
        # You might need to modify this part depending on your message structure and requirements.
        data = message_body + '\n'
        data_bytes = data.encode('utf-8')

        # Put the record into the Firehose delivery stream
        response = firehose_client.put_record(
            DeliveryStreamName=STREAM_NAME,
            Record={
                'Data': data_bytes
            }
        )
        
        logger.info(f"Response from Firehose: {response}")

    return {
        'statusCode': 200,
        'body': json.dumps('Messages successfully processed and sent to Kinesis Firehose')
    }
