import boto3
import multiprocessing
import logging
import time

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.DEBUG)


class Consumer(multiprocessing.Process):
    """Child process for consuming from work queue and updating DynamoDB.

    """
    def __init__(self, group=None, target=None, name=None, args=(), kwargs={}):
        super(Consumer, self).__init__(group, target, name, args, kwargs)
        self.cliargs = kwargs['cliargs']
        self.awsargs = kwargs['awsargs']
        self.done = kwargs['done']
        self.error_exit = kwargs['error_exit']
        self.id = kwargs['id']
        self.work_queue = kwargs['work_queue']
        self.updated_count = kwargs['updated_count']

    @property
    def queue_size(self):
        try:
            return self.work_queue.qsize()
        except NotImplementedError:
            return -1

    def update_item(self, item):
        LOGGER.debug('Updating serial %s', item['Serial'])
        self.client.update_item(
            TableName=self.cliargs.table,
            Key={
                'Serial': {
                    'S': item['Serial']
                }
            },
            UpdateExpression='SET Timezone = :tz',
            ConditionExpression='attribute_not_exists(Timezone)',
            ExpressionAttributeValues={
                ':tz': {
                    'S': 'Europe/London'
                }
            },
            ReturnValues='NONE'
        )
        with self.updated_count.get_lock():
            self.updated_count.value += 1

    def run(self):
        """Executes on start, gets item from queue to update on table"""
        self.client = boto3.client('dynamodb',
            region_name=self.awsargs['region'],
            endpoint_url=self.awsargs['endpoint'],
            aws_access_key_id=self.awsargs['key'],
            aws_secret_access_key=self.awsargs['secret'])
        LOGGER.info('Starting to work on updating %s as process id %s of %s',
                    self.cliargs.table, self.id, self.cliargs.consumers)
        
        while True:
            if self.error_exit.is_set():
                break
            try:
                record = self.work_queue.get(block=False, timeout=10)
                self.update_item(record)
            except Exception:
                pass

        self.done.set()
        return