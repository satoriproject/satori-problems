# vim:ts=4:sts=4:sw=4:et
import logging

from satori.client.common import want_import
want_import(globals(), '*')
from satori.tools import catch_exceptions, options, setup

from render_statement import render_statement
from temporary_submit import temporary_submit, verbose_result
from sync import sync

# @catch_exceptions
def main():
    subparsers = options.add_subparsers()
    
    temporary_submit_parser = subparsers.add_parser('test')
    temporary_submit_parser.set_defaults(command=temporary_submit)
    temporary_submit_parser.add_argument('TESTSUITE')
    temporary_submit_parser.add_argument('SOLUTIONS', nargs='+')
    temporary_submit_parser.add_argument('-t', '--time')
    temporary_submit_parser.add_argument('-v', '--verbose', action='store_const', const=True)
    temporary_submit_parser.add_argument('-2', '--results2d', action='store_const', const=True)
    temporary_submit_parser.add_argument('--store_io', action='store_const', const=True)
    temporary_submit_parser.add_argument('--length_limit', type=int, default=4096)

    verbose_result_parser = subparsers.add_parser('result')
    verbose_result_parser.set_defaults(command=verbose_result)
    verbose_result_parser.add_argument('TSID')
    verbose_result_parser.add_argument('--length_limit', type=int, default=4096)

    render_statement_parser = subparsers.add_parser('render')
    render_statement_parser.set_defaults(command=render_statement)
    render_statement_parser.add_argument('STATEMENT')
    render_statement_parser.add_argument('ATTACHMENTS', nargs='*')
    render_statement_parser.add_argument('OUTPUT')

    sync_parser = subparsers.add_parser('sync')
    sync_parser.set_defaults(command=sync)
    sync_parser.add_argument('MAPPING')

    opts = setup(logging.INFO)
    opts.command(opts)
