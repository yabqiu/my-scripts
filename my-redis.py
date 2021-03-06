#!/usr/bin/env python

import sys, io, os
from optparse import OptionParser
from redis import Redis, exceptions
import gzip
import tempfile
import json
import functools
import json

print = functools.partial(print, flush=True)
verbose = False
supported_commands = ['get', 'type', 'hget', 'hgetall', 'smembers', 'info', 'memory_usage']


def main():
    parser = OptionParser(add_help_option=False)
    parser.add_option("-h", "--hostname", default='127.0.0.1', metavar='<hostname>',
                      help='Server hostname (default: 127.0.0.1).')
    parser.add_option('-g', '--group', metavar='<cluster_name>',
                      help='AWS ElastiCache cluster name, <replicationGroupId>')
    parser.add_option('--force-parse-endpoint', action='store_true', default=False,
                      help='Force parse AWS ElasticCache cluster name')
    parser.add_option('-a', '--password', metavar='<password>', help='Password to use when connecting to the server.')
    parser.add_option("-n", '--db', metavar='<db>', default=0, type='int', help='Redis database number.')
    parser.add_option('--scan', metavar='<pattern>', help='List all keys using the SCAN command for the pattern.')
    parser.add_option('-t', '--top', default=sys.maxsize, metavar='<num>', type='int',
                      help='List top N found keys when using --scan.')
    parser.add_option('-m', '--show-memory-usage', default=False, action='store_true',
                      help='Calculate memory usage when using --scan')
    parser.add_option('-v', '--verbose', action='store_true', default=False, help='Verbose mode')
    parser.add_option('--help', action='help', help='Print this help information.')

    if len(sys.argv) < 2:
        parser.print_help()
        sys.exit(1)

    (options, args) = parser.parse_args()

    redis_command = parse_redis_command()

    global verbose
    verbose = options.verbose

    if redis_command is None and options.scan == os.path.basename(__file__):
        return

    if options.group:
        primary, reader = parse_hostname_by_elasticache_cluster_name(options.group.split('/')[0],
                                                                     options.force_parse_endpoint)
        verbose_info('PrimaryEndpoint: {}'.format(primary))
        verbose_info('ReaderEndpoint:  {}'.format(reader))

        options.hostname = primary
        if '/' in options.group:
            options.db = int(options.group.split('/')[1])

    client:Redis = Redis(host=options.hostname, db=options.db, password=options.password)

    try:
        client.ping()
    except exceptions.ConnectionError:
        print(f'Cannot connect to {options.hostname}')
        sys.exit(1)

    if redis_command:
        execute_command(client, redis_command)
    elif options.scan:
        scan_keys(client, options.scan, options.top, options.show_memory_usage)


def parse_redis_command():
    redis_command = []
    found__ = False
    for arg in sys.argv:
        if found__:
            redis_command.append(arg)
        if arg == '--':
            found__ = True
    if len(redis_command) == 0:
        return None
    return ' '.join(redis_command)


def redis_check(func):
    def wrapper(*args):
        try:
            func(*args)
        except exceptions.ResponseError as er:
            print(er.args[0])
    return wrapper


def parse_hostname_by_elasticache_cluster_name(cluster, force):
    parsed_config = os.path.join(tempfile.gettempdir(), 'parsed_endpoints.txt')
    if not force:
        if os.path.exists(parsed_config):
            with open(parsed_config) as fd:
                for line in fd:
                    endpoint = line.rstrip().split("=")
                    if endpoint[0] == cluster:
                        primary_and_reader = endpoint[1].split(',')
                        return (primary_and_reader[0], primary_and_reader[1])

    import boto3
    import fileinput
    ec = boto3.client('elasticache')
    result = ec.describe_replication_groups(ReplicationGroupId=cluster)
    primary = result['ReplicationGroups'][0]['NodeGroups'][0]['PrimaryEndpoint']['Address']
    reader = result['ReplicationGroups'][0]['NodeGroups'][0]['ReaderEndpoint']['Address']

    if not os.path.exists(parsed_config):
        with open(parsed_config, 'a') as fd:
            fd.write(f'{cluster}={primary},{reader}')
    else:
        for line in fileinput.input(parsed_config, inplace=True):
            if line.rstrip().split('=')[0] == cluster:
                line = f'cluster={primary},{reader}'
        fileinput.close()

    return (primary, reader)


def scan_keys(client: Redis, pattern, top: int, show_memory_usage):
    memory_usage = 0
    limit = 0
    count = 20000
    result = client.scan(0, pattern, count)
    cursor = result[0]
    while True:
        batch = []
        for key in result[1]:
            batch.append(key)
            limit += 1

            if show_memory_usage:
                usage = client.memory_usage(key.decode())
                print('{:<25s} {}'.format(key.decode(), usage))
                memory_usage += usage
            else:
                print(key.decode())

            if limit == top:
                break

        if limit == top:
            break

        if cursor != 0:
            result = client.scan(cursor, pattern, count)
            cursor = result[0]
        else:
            break

    if show_memory_usage:
        print('Summary: keys: {}, total memory usage: {}'.format(limit, memory_usage))
    else:
        print('Summary: keys: {}'.format(limit))


def execute_command(client: Redis, command_str: str):
    split = command_str.split()
    command = split[0].lower()

    if command in supported_commands:
        globals()[command + '_command'](client, split[1:])
    else:
        # print("Unsupported command '{}'".format(split[0]), file=sys.stderr)
        unknown_command(client, split[:])


def unknown_command(client: Redis, param):
    result = client.execute_command(*param)
    print(json.dumps(result, indent=4))


@redis_check
def get_command(client: Redis, param):
    if len(param) == 0:
        print('require key', file=sys.stderr)
        sys.exit(1)

    key = param[0]
    data = client.get(key)
    if data is not None:
        verbose_info("Object size: {}".format(len(data)))
        if data[0:2] == bytes.fromhex('1f8b'):
            verbose_info("gzip format")
            verbose_info("bytes: {}".format(data))
            data = gzip.decompress(data)
        elif data[0:4] == bytes.fromhex('4f626a01'):
            verbose_info("Avro format")
            verbose_info("bytes: {}".format(data))
            data = deserialize_avro(data)
        try:
            print(data.decode())
        except UnicodeDecodeError:
            print(data)
    else:
        print(data)


@redis_check
def type_command(client: Redis, param):
    if len(param) == 0:
        print('require key', file=sys.stderr)
        sys.exit(1)
    key = param[0]
    print(client.type(key).decode())


@redis_check
def hget_command(client: Redis, param):
    if len(param) < 2:
        print('require key and field', file=sys.stderr)
        sys.exit(1)
    result = client.hget(param[0], param[1])
    value = '(nil)' if result is None else f'"{result.decode()}"'
    print(value)


@redis_check
def hgetall_command(client: Redis, param):
    if len(param) == 0:
        print('key required', file=sys.stderr)
        sys.exit(1)
    key = param[0]
    result = client.hgetall(key)
    if len(result) == 0:
        print('(empty list or set')
    else:
        count = 0
        for k, v in result.items():
            count += 1
            print(f'{count}) "{k.decode()}"')
            count += 1
            print(f'{count}) "{v.decode()}"')


@redis_check
def smembers_command(client: Redis, param):
    if len(param) == 0:
        print('key required', file=sys.stderr)
        sys.exit(1)
    key = param[0]
    result = client.smembers(key)
    if len(result) == 0:
        print('(empty list or set')
    else:
        count = 0
        for value in result:
            count += 1
            print(f'{count}) "{value.decode()}"')


@redis_check
def info_command(client: Redis, param):
    info = client.info(section=None if len(param) == 0 else param[0])
    for key, value in info.items():
        print(f'{key}: {value}')


@redis_check
def memory_usage_command(client: Redis, param):
    if len(param) == 0:
        print('require key', file=sys.stderr)
        sys.exit(1)
    key = param[0]
    print(client.memory_usage(key))


def deserialize_avro(data):
    from fastavro import reader

    buf = io.BytesIO(data)
    lines = []
    for record in reader(buf):
        lines.append(json.dumps(record))
    return '\n'.join(lines).encode()


def verbose_info(msg):
    if verbose:
        print(msg, file=sys.stderr)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print(' Interrupted')
        sys.exit(0)
