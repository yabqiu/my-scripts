#!/usr/bin/env python

import sys
import io
from optparse import OptionParser
from redis import Redis, exceptions
import gzip

verbose = False


def main():
    parser = OptionParser(add_help_option=False)
    parser.add_option("-h", "--hostname", default='127.0.0.1', help='Server hostname (default: 127.0.0.1).')
    parser.add_option('-g', '--group', help='AWS ElastiCache cluster name, will use primary node to access')
    parser.add_option('-a', '--password', help='Password to use when connecting to the server.')
    parser.add_option('--help', action='help')
    parser.add_option("-n", '--db', default=0, type='int', help='Database number.')
    parser.add_option('--scan', action='store_true', help='List all keys using the SCAN command.')
    parser.add_option('--pattern', default='*', help='Useful with --scan to specify a SCAN pattern.')
    parser.add_option('-c', '--command', help='Execute Redis command')
    parser.add_option('--verbose', action='store_true', default=False, help='Verbose mode')

    if len(sys.argv) < 2:
        parser.print_help()
        sys.exit(1)

    (options, args) = parser.parse_args()

    global verbose
    verbose = options.verbose

    if options.group:
        options.hostname = parse_hostname_by_elasticache_cluster_name(options.group.split('/')[0])
    if '/' in options.group:
        options.db = int(options.group.split('/')[1])

    client = Redis(host=options.hostname, db=options.db, password=options.password)
    if options.command:
        execute_command(client, options.command)
    elif options.scan:
        scan_keys(client, options.pattern)


def parse_hostname_by_elasticache_cluster_name(cluster):
    import boto3
    ec = boto3.client('elasticache')
    result = ec.describe_replication_groups(ReplicationGroupId=cluster)
    primary = result['ReplicationGroups'][0]['NodeGroups'][0]['PrimaryEndpoint']['Address']
    verbose_info('Get endpoint of {}: {}'.format(cluster, primary))
    return primary


def scan_keys(client: Redis, pattern):
    count = 20000
    result = client.scan(0, pattern, count)
    cursor = result[0]
    while cursor != 0:
        for k in result[1]:
            print(k.decode(), flush=True)

        result = client.scan(cursor, pattern, count)
        cursor = result[0]


def execute_command(client: Redis, command_str: str):
    split = command_str.split()
    command = split[0].upper()

    if command == 'GET':
        get_command(client, split[1:])
    elif command == 'TYPE':
        type_command(client, split[1:])
    else:
        print("Unsupported command '{}'".format(split[0]), file=sys.stderr)


def get_command(client: Redis, param):
    if len(param) == 0:
        print('require key', file=sys.stderr)

    key = param[0]
    try:
        data = client.get(key)
        if data is not None:
            verbose_info("object size: {}".format(len(data)))
            if data[0:2] == bytes.fromhex('1f8b'):
                verbose_info("found gzip format data at key {}".format(key))
                verbose_info("bytes: {}".format(data))
                data = gzip.decompress(data)
            elif data[0:4] == bytes.fromhex('4f626a01'):
                verbose_info("found Avro format at at key {}".format(key))
                verbose_info("bytes: {}".format(data))
                data = deserialize_avro(data)
            try:
                print(data.decode(), flush=True)
            except UnicodeDecodeError:
                print(data)
        else:
            print(data)
    except exceptions.ResponseError as er:
        print(er.args[0])


def type_command(client: Redis, param):
    if len(param) == 0:
        print('require key', file=sys.stderr)
    key = param[0]
    print(client.type(key).decode())


def deserialize_avro(data):
    from fastavro import reader
    import json

    buf = io.BytesIO(data)
    lines = []
    for record in reader(buf):
        lines.append(json.dumps(record))
    return '\n'.join(lines).encode()


def verbose_info(msg):
    if verbose:
        print(msg, flush=True, file=sys.stderr)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        sys.exit(0)
