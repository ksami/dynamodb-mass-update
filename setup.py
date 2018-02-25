#!/usr/bin/env python3

import logging
import boto3
import time

TABLE_NAME='test_table'
DEFAULT_TZ='Europe/London'

LOGGER = logging.getLogger(__name__)

class Setup(object):
    def __init__(self):
        for logger in {
                'boto3', 'botocore', 'requests',
                'botocore.vendored.requests.packages.urllib3.connectionpool'}:
            logging.getLogger(logger).setLevel(logging.INFO)
        LOGGER.setLevel(logging.INFO)
        LOGGER.debug('Starting up')

        self.db = boto3.client('dynamodb',
            region_name='eu-west-1',
            endpoint_url='http://192.168.99.100:4569',
            aws_access_key_id='localstack',
            aws_secret_access_key='localstack')
    
    def setup(self):
        LOGGER.info('Creating table')
        self.createTable()
        LOGGER.info('Seeding table')
        self.seedTable()
        LOGGER.info('=== Done ===')


    def createTable(self):
        self.db.create_table(
            AttributeDefinitions=[
                {
                    'AttributeName': 'Serial',
                    'AttributeType': 'S'
                }
            ],
            TableName=TABLE_NAME,
            KeySchema=[
                {
                    'AttributeName': 'Serial',
                    'KeyType': 'HASH'
                }
            ],
        ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )

    def seedTable(self):
        for i in range(10):
            serial='serial-' + str(i)
            item={
                'Serial': {
                    'S': serial
                }
            }
            if i % 5 == 0:
                item['Timezone']={
                    'S': 'Asia/Tokyo'
                }
            self.db.put_item(
                Item=item,
                TableName=TABLE_NAME
            )
