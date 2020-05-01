#!/usr/bin/env python

import sys
from optparse import OptionParser
from redis import Redis
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

    (options, args) = parser.parse_args()

    global verbose
    verbose = options.verbose

    if options.group:
        options.hostname = parse_hostname_by_elasticache_cluster_name(options.group.split('/')[0])
    if '/' in options.group:
        options.db = int(options.group.split('/')[1])

    client = Redis(host=options.hostname, db=options.db)
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


def execute_command(client: Redis, command: str):
    split = command.split()
    if len(split) == 0:
        print('require key', file=sys.stderr)

    if split[0].upper() == 'GET':
        get_command(client, split[1])
    else:
        print("Unsupported command '{}'".format(split[0]), file=sys.stderr)


def get_command(client: Redis, key):
    data = client.get(key)
    if data is not None:
        if data[0:2] == bytes.fromhex('1f8b'):
            verbose_info("found gzip format data at key {}".format(key))
            data = gzip.decompress(data)
        print(data.decode(), flush=True)
    else:
        print(data)


def verbose_info(msg):
    if verbose:
        print(msg, flush=True, file=sys.stderr)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        sys.exit(0)
