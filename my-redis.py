#!/usr/bin/env python

import sys, io, os
from optparse import OptionParser
from redis import Redis, exceptions
import gzip
import tempfile
import json
import functools

print = functools.partial(print, flush=True)
verbose = False


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
    parser.add_option('-t', '--top', default=sys.maxsize, metavar='<num>', help='List top N found keys when using --scan.')
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
        options.hostname = parse_hostname_by_elasticache_cluster_name(options.group.split('/')[0],
                                                                      options.force_parse_endpoint)
        verbose_info('Endpoint: {}'.format(options.hostname))
    if '/' in options.group:
        options.db = int(options.group.split('/')[1])

    client = Redis(host=options.hostname, db=options.db, password=options.password)
    if redis_command:
        execute_command(client, redis_command)
    elif options.scan:
        scan_keys(client, options.scan, int(options.top))


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
                        return endpoint[1]

    import boto3
    import fileinput
    ec = boto3.client('elasticache')
    result = ec.describe_replication_groups(ReplicationGroupId=cluster)
    primary = result['ReplicationGroups'][0]['NodeGroups'][0]['PrimaryEndpoint']['Address']

    if not os.path.exists(parsed_config):
        with open(parsed_config, 'a') as fd:
            fd.write(f'{cluster}={primary}')
    else:
        for line in fileinput.input(parsed_config, inplace=True):
            if line.rstrip().split('=')[0] == cluster:
                line = f'cluster={primary}'
        fileinput.close()

    return primary


def scan_keys(client: Redis, pattern, top: int):
    limit = 0
    count = 20000
    result = client.scan(0, pattern, count)
    cursor = result[0]
    while cursor != 0:
        for k in result[1]:
            print(k.decode())
            limit += 1
            if limit == top:
                return

        result = client.scan(cursor, pattern, count)
        cursor = result[0]


def execute_command(client: Redis, command_str: str):
    split = command_str.split()
    command = split[0].upper()

    if command == 'GET':
        get_command(client, split[1:])
    elif command == 'TYPE':
        type_command(client, split[1:])
    elif command == 'HGET':
        hget_command(client, split[1:])
    elif command == 'HGETALL':
        hgetall_command(client, split[1:])
    else:
        print("Unsupported command '{}'".format(split[0]), file=sys.stderr)


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
    if len(result) == 0:
        print('(nil)')
    else:
        count = 0
        for k, v in result.items():
            count += 1
            print(f'{count}) "{k.decode()}"')
            count += 1
            print(f'{count}) "{v.decode()}"')


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
