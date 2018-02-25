import boto3
import multiprocessing
import logging
import time

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.DEBUG)


class Producer(multiprocessing.Process):
    """Child process for paginating from DynamoDB and putting rows in the work
    queue.

    """
    def __init__(self, group=None, target=None, name=None, args=(), kwargs={}):
        super(Producer, self).__init__(group, target, name, args, kwargs)
        self.cliargs = kwargs['cliargs']
        self.awsargs = kwargs['awsargs']
        self.done = kwargs['done']
        self.error_exit = kwargs['error_exit']
        self.segment = kwargs['segment']
        self.units = kwargs['unit_count']
        self.work_queue = kwargs['work_queue']

    @property
    def queue_size(self):
        try:
            return self.work_queue.qsize()
        except NotImplementedError:
            return -1

    def run(self):
        """Executes on start, paginate through the table"""
        client = boto3.client('dynamodb',
            region_name=self.awsargs['region'],
            endpoint_url=self.awsargs['endpoint'],
            aws_access_key_id=self.awsargs['key'],
            aws_secret_access_key=self.awsargs['secret'])
        paginator = client.get_paginator('scan')
        LOGGER.info('Starting to page through %s for segment %s of %s',
                    self.cliargs.table, self.segment, self.cliargs.producers)
        for page in paginator.paginate(TableName=self.cliargs.table,
                                       TotalSegments=self.cliargs.producers,
                                       ConsistentRead=True,
                                       PaginationConfig={'PageSize': 500},
                                       ReturnConsumedCapacity='TOTAL',
                                       Segment=self.segment):
            for record in [_unmarshall(item) for item in
                           page.get('Items', [])]:
                self.work_queue.put(record)
                if self.error_exit.is_set():
                    break
            # with self.units.get_lock():
            #     self.units.value += page['ConsumedCapacity']['CapacityUnits']
            if self.error_exit.is_set():
                break
        self.done.set()
        return




def _to_number(value):
    """Convert the string containing a number to a number

    :param str value: The value to convert
    :rtype: float|int

    """
    return float(value) if '.' in value else int(value)


def _unmarshall(values):
    """Transform a response payload from DynamoDB to a native dict

    :param dict values: The response payload from DynamoDB
    :rtype: dict
    :raises ValueError: if an unsupported type code is encountered

    """
    return dict([(k, _unmarshall_dict(v)) for k, v in values.items()])


def _unmarshall_dict(value):
    """Unmarshall a single dict value from a row that was returned from
    DynamoDB, returning the value as a normal Python dict.

    :param dict value: The value to unmarshall
    :rtype: mixed
    :raises ValueError: if an unsupported type code is encountered

    """
    key = list(value.keys()).pop()
    if key == 'B':
        return base64.b64decode(value[key].encode('ascii'))
    elif key == 'BS':
        return set([base64.b64decode(v.encode('ascii'))
                    for v in value[key]])
    elif key == 'BOOL':
        return value[key]
    elif key == 'L':
        return [_unmarshall_dict(v) for v in value[key]]
    elif key == 'M':
        return _unmarshall(value[key])
    elif key == 'NULL':
        return None
    elif key == 'N':
        return _to_number(value[key])
    elif key == 'NS':
        return set([_to_number(v) for v in value[key]])
    elif key == 'S':
        return value[key]
    elif key == 'SS':
        return set([v for v in value[key]])
    raise ValueError('Unsupported value type: %s' % key)

