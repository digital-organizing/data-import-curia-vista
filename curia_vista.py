#!/usr/bin/env python3
import argparse
import logging
import sys

import requests

import pyodata
from odata2sql import command_dot, command_dump, command_init, command_sync, command_benchmark_aiohttp, \
    command_benchmark_threading
from odata2sql.odata import Context
from pyodata.v2.model import Config

log = logging.getLogger(__name__)

SYNC_BY_FK = {
    'Voting': 'Vote'
}


def context_from_args(args):
    if args.requests_cache:
        import requests_cache
        requests_cache.install_cache(args.requests_cache)

    try:
        language_filter = ' or '.join(f"(Language eq '{language}')" for language in args.language)
    except (TypeError, AttributeError):
        language_filter = None

    client = pyodata.Client(args.url, requests.Session(), config=Config(retain_null=True))
    return Context(client,
                   getattr(args, 'include', set()),
                   getattr(args, 'skip', set()),
                   args.url,
                   language_filter,
                   )


def main():
    parser_top_level = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser_top_level.add_argument('-v', '--verbose',
                                  dest='verbose_count',
                                  action='count',
                                  default=0,
                                  help='Increase log verbosity for each occurrence.')
    parser_top_level.add_argument('--url', type=str, default='https://ws.parlament.ch/odata.svc')
    parser_top_level.add_argument('--requests-cache', type=str,
                                  help="Cache HTTP requests in <cache>.sqlite. Useful to speed up development.")
    subparsers = parser_top_level.add_subparsers(dest='command', required=True)
    benchmark_aiohttp_parser = subparsers.add_parser('benchmark-aiohttp', help='Benchmark OData server using aiohttp')
    benchmark_threading_parser = subparsers.add_parser('benchmark-threading',
                                                       help='Benchmark OData server using multiple threads')
    init_parser = subparsers.add_parser('init', help='Initialize database')
    sync_parser = subparsers.add_parser('sync', help='Synchronize database from scratch')
    update_parser = subparsers.add_parser('update', help='Incrementally update database')
    dot_parser = subparsers.add_parser('dot', help='Show dependencies between entity types')
    dump_parser = subparsers.add_parser('dump', help='Show dependencies between entity types')
    for parser in [benchmark_aiohttp_parser, benchmark_threading_parser, init_parser, sync_parser, update_parser,
                   dot_parser, dump_parser]:
        parser.add_argument('--include', type=str, nargs='+',
                            help='Entity types to work on, dependencies added as needed. All if unspecified.')
    for parser in [benchmark_aiohttp_parser, benchmark_threading_parser, sync_parser, update_parser, dump_parser]:
        parser.add_argument('--skip', type=str, nargs='+', help='Forcefully ignore the listed entities. Beware!')
    for parser in [init_parser, sync_parser, update_parser]:
        parser.add_argument("-u", '--user', type=str, default='curiavista', help='Database user (default: %(default)s)')
        parser.add_argument("-H", '--host', type=str, default='127.0.0.1',
                            help='PostgreSQL host (default: %(default)s)')
        parser.add_argument("-P", '--port', type=int, default=5432, help='Port to connect on (default: %(default)s)')
        parser.add_argument("-p", '--password', type=str, help='Attempting ~/.pgpass if not provided')
        parser.add_argument("-d", '--database', dest='dbname', type=str, default='curiavista',
                            help='Database name (default: %(default)s)')
    for parser in [benchmark_aiohttp_parser, benchmark_threading_parser, sync_parser, update_parser]:
        parser.add_argument('--language', type=str, nargs='+',
                            help='Restrict import to specified language(s): DE, FR, IT, RM, EN. (default: all)')
    for parser in [init_parser]:
        parser.add_argument("-f", '--force', action='store_true', help='Erase all preexisting content in database')
    for parser in [dump_parser]:
        parser.add_argument('--ipython', action='store_true', help='Drop into IPython shell')
    args = parser_top_level.parse_args()

    log_level = max(3 - args.verbose_count, 0) * 10
    log.info(f"Setting loglevel to {log_level}")
    logging.basicConfig(stream=sys.stderr, level=log_level, format='%(asctime)s %(name)s %(levelname)s %(message)s')

    odata = context_from_args(args)
    if args.command == 'benchmark-aiohttp':
        command_benchmark_aiohttp.work(odata, args)
    if args.command == 'benchmark-threading':
        command_benchmark_threading.work(odata, args)
    if args.command == 'dump':
        command_dump.work(odata, args)
    elif args.command == 'dot':
        command_dot.work(odata, args)
    if args.command == 'init':
        command_init.work(odata, args)
    if args.command == 'sync':
        command_sync.work(odata, args, SYNC_BY_FK)
    if args.command == 'update':
        raise NotImplemented('Update functionality is not yet implemented!')


if __name__ == '__main__':
    main()
