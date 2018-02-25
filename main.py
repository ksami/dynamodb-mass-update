import logging
import argparse
import multiprocessing
import time
import sys
from producer import Producer
from consumer import Consumer
from setup import Setup

__version__ = '0.0.1'
logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)


def main():
    """Setup and run the backup."""
    awsargs = {
        'region': 'eu-west-1',
        'endpoint': 'http://192.168.99.100:4569',
        'key': 'localstack',
        'secret': 'localstack'
    }

    args = _parse_cli_args()
    level = logging.DEBUG if args.verbose else logging.INFO
    if args.verbose:
        for logger in {
                'boto3', 'botocore', 'requests',
                'botocore.vendored.requests.packages.urllib3.connectionpool'}:
            logging.getLogger(logger).setLevel(level)
    LOGGER.error('STARTTINGG')
    LOGGER.info('Sleeping for 5 seconds')
    time.sleep(5)
    LOGGER.info('Woke up')
    setup = Setup()
    setup.setup()
    _execute(args, awsargs)


def _execute(args, awsargs):
    """What executes when your application is run. It will spawn the
    specified number of threads, reading in the data from the specified file,
    adding them to the shared queue. The worker threads will work the queue,
    and the application will exit when the queue is empty.

    :param argparse.namespace args: The parsed cli arguments

    """
    error_exit = multiprocessing.Event()
    work_queue = multiprocessing.Queue()
    units = multiprocessing.Value('f', 0, lock=True)
    updated_count = multiprocessing.Value('L', 0, lock=True)

    producers = []
    LOGGER.info('Starting %i producers', args.producers)
    for index in range(0, args.producers):
        done = multiprocessing.Event()
        prod = Producer(kwargs={'done': done,
                               'error_exit': error_exit,
                               'segment': index,
                               'cliargs': args,
                               'awsargs': awsargs,
                               'unit_count': units,
                               'work_queue': work_queue})
        prod.start()
        producers.append((prod, done))

    consumers = []
    LOGGER.info('Starting %i consumers', args.consumers)
    for index in range(0, args.consumers):
        done = multiprocessing.Event()
        con = Consumer(kwargs={'done': done,
                               'error_exit': error_exit,
                               'id': index,
                               'cliargs': args,
                               'awsargs': awsargs,
                               'updated_count': updated_count,
                               'unit_count': units,
                               'work_queue': work_queue})
        con.start()
        consumers.append((con, done))


    start_time = time.time()

    # All of the data has been added to the queue, wait for things to finish
    LOGGER.debug('Waiting for producers to finish')
    while any([p.is_alive() for (p, d) in producers]):
        try:
            time.sleep(0.1)
        except KeyboardInterrupt:
            error_exit.set()
            break
    LOGGER.info('All producers dead')

    # Wait till queue is cleared, then send exit
    while work_queue.qsize() > 0:
        LOGGER.debug('Work queue size is %s, updated_count is %s', work_queue.qsize(), updated_count.value)
        time.sleep(0.1)
    LOGGER.info('Sending exit signal')
    error_exit.set()

    LOGGER.debug('Waiting for consumers to finish')
    while any([p.is_alive() for (p, d) in consumers]):
        try:
            time.sleep(0.1)
        except KeyboardInterrupt:
            error_exit.set()
            break
    LOGGER.info('All consumers dead')

    LOGGER.info('Updated {:d} records, consuming {:,} DynamoDB units in '
                '{:.2f} seconds'.format(
                    updated_count.value,
                    units.value,
                    time.time() - start_time))
    sys.exit(0)


def _parse_cli_args():
    """Construct the CLI argument parser and return the parsed the arguments.

    :rtype: argparse.namespace

    """
    parser = argparse.ArgumentParser(
        description='Backup a DynamoDB table',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('-p', '--producers', default=1, type=int,
                        metavar='COUNT',
                        help='Total number of producers for scan.')

    parser.add_argument('-c', '--consumers', default=1, type=int,
                        metavar='COUNT',
                        help='Total number of consumers for update.')


    parser.add_argument('-v', '--verbose', action='store_true',
                        help='Verbose logging output')
    parser.add_argument('--version', action='version',
                        version='%(prog)s {}'.format(__version__))

    parser.add_argument('table', action='store', help='DynamoDB table name')
    return parser.parse_args()


if __name__ == '__main__':
    main()