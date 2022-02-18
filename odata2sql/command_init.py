import logging
from pathlib import Path

from psycopg2._psycopg import cursor

from odata2sql.odata import Context
from odata2sql.schema_generator import LeanAndMean
from odata2sql.sql import database_connection

log = logging.getLogger(__name__)

# Needs to be in sync with the schemas being created by various scripts (e.g. in the pre-init.d folder)
SCHEMAS = [
    'inconsistent',
    'odata',
    'odata_constrain_violation',  # No longer used, but keep here to drop from old databases
    'private',
    'stable',
]


def _run_sql_scripts(cur: cursor, directory_name: Path) -> None:
    directory_name = directory_name.resolve()  # Allow caller to be lenient
    if not directory_name.is_dir():
        raise ValueError(f'Not a valid path: {directory_name}')
    for entry in sorted(directory_name.glob('*.sql')):
        with open(entry) as f:
            script_content = f.read()
        log.info(f'Executing script {entry}')
        log.debug(f'Content of {entry}: {script_content}')
        cur.execute(script_content)


def work(context: Context, args):
    """Initialize database structure"""
    with database_connection(args) as con, con.cursor() as cur:
        if args.force:
            cur.execute(f'DROP SCHEMA IF EXISTS {", ".join(SCHEMAS)} CASCADE;')
        log.info(f'Run pre-init scripts')
        _run_sql_scripts(cur, Path(__file__).parent.joinpath('pre-init.d'))
        for schema_generator in [LeanAndMean(context)]:
            schema = schema_generator.odata_to_ddl()
            log.info(f'Using schema generator "{schema_generator}')
            log.debug(f'Schema being applied: {schema}')
            cur.execute(schema)
        log.info(f'Run post-init scripts')
        _run_sql_scripts(cur, Path(__file__).parent.joinpath('post-init.d'))
        log.info(f'Run service specific scripts')
        _run_sql_scripts(cur, Path(__file__).parent.joinpath('../post-init.d'))
