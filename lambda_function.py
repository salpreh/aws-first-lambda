import os
import json
import csv
import logging
from urllib.parse import unquote_plus

import boto3

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.DEBUG)

def lambda_handler(event, context):
    LOGGER.info("Event_data: %s",event)
    keys = []
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = unquote_plus(record['s3']['object']['key'])

        data = fetch_and_process_file(bucket, key)
        move_file_to_done(bucket, key)
        keys.append(key)

    return success_res(keys)

def fetch_and_process_file(bucket: str, key: str) -> dict:
    s3_client = boto3.client('s3')

    res = s3_client.get_object(
        Bucket=bucket,
        Key=key
    )

    line_iter = res['Body'].iter_lines()

    csv_data = csv.reader(decoder_wraper(line_iter), delimiter=';')
    header = csv_data.__next__()
    data_buff = [';'.join(header)]
    LOGGER.debug("CSV header: %s", header)
    for line in csv_data:
        LOGGER.debug("Processing CSV line: %s", line)
        ev_data = { h:l for h, l in zip(header, line)}

        LOGGER.debug("generated event: %s", ev_data)
        send_sqs_msg(ev_data, {'file-origin': key})
        data_buff.append(';'.join(line))

    return '\n'.join(data_buff)

def send_sqs_msg(body: dict, headers: dict=None):
    sqs_cli = boto3.client('sqs')

    msg_attributes = {}
    if headers:
        for k, v in headers.items():
            msg_attributes[k] = {
                'DataType': 'String',
                'StringValue': str(v)
            }

    sqs_cli.send_message(
        QueueUrl=os.getenv('SQS_URL'),
        MessageBody=json.dumps(body),
        MessageAttributes=msg_attributes
    )

def move_file_to_done(bucket: str, key: str):
    dest_path = os.environ['S3_DEST_PATH']
    dst_key = "{}/{}".format(dest_path, key.split('/')[-1])

    s3_client = boto3.client('s3')
    s3_client.copy_object(
        Bucket=bucket,
        Key=dst_key,
        CopySource={
            'Bucket': bucket,
            'Key': key
        }
    )

    s3_client.delete_object(
        Bucket=bucket,
        Key=key
    )

def success_res(resources: list):
    return {
        'code': 200,
        'resource': resources,
    }

def decoder_wraper(iterable):
    for it in iterable:
        yield str(it, encoding='utf8')

