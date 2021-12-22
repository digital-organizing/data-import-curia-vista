import datetime
import logging
import sys
from typing import Dict, List

from psycopg2 import ProgrammingError
from psycopg2.extras import execute_batch

from odata2sql.sql import database_connection, to_pg_name
from odata2sql.odata import Context
from pyodata.v2.model import EntityType

log = logging.getLogger(__name__)


def update_db(db_connection, table_name: str, sql_column_names: List[str], sql_row_data: List):
    """Update multiple """
    pg_table_name = to_pg_name(table_name)
    table_name_pkey = to_pg_name(table_name + '_pkey')
    statement = (f'INSERT INTO odata.{pg_table_name} ({", ".join(sql_column_names)})'
                 f' VALUES ({", ".join(["%s"] * len(sql_column_names))}) '
                 f' ON CONFLICT ON CONSTRAINT {table_name_pkey} DO UPDATE SET {", ".join([f"{c} = EXCLUDED.{c}" for c in sql_column_names])};')
    log.debug(f'Running "{statement} on {len(sql_row_data)} rows')
    db_connection.commit()
    with db_connection.cursor() as cur:
        def do_many(rows):
            try:
                execute_batch(cur, statement, rows)
                db_connection.commit()
                return
            except ProgrammingError as e:
                db_connection.rollback()
                log.fatal(e)
                log.fatal(f"Failed statement: {statement}")
                sys.exit(-1)
            except Exception as e:
                db_connection.rollback()
                if len(rows) == 1:
                    log.error(f"{str(rows[0])}: {e}")
                    return

            do_many(rows[len(rows) // 2:])
            do_many(rows[:len(rows) // 2])

        do_many(sql_row_data)


EPOCH = datetime.datetime.utcfromtimestamp(0)


def _parse_date(data: str):
    """
    Convert  "ASP.Net JSON Date" (e.g. '/Date(1293368772797)/') to a timestamp string understood by the SQL driver.
    :param data: A "ASP.Net JSON Date" string
    :return:
    """
    time_part = data.split('(')[1].split(')')[0]
    if "+" in time_part:
        adjusted_seconds = int(time_part[:-5]) / 1000 + int(time_part[-5:]) / 100 * 3600
    else:
        adjusted_seconds = int(time_part) / 1000

    dt = EPOCH + datetime.timedelta(seconds=adjusted_seconds)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def get_entity_type_property_names(entity_type: EntityType) -> List[str]:
    """Name of all properties for a given entity type"""
    return [p.name for p in entity_type.proprties()]


def get_gp_column_names_from_entity_type(entity_type: EntityType) -> List[str]:
    """SQL column names derived from an entity type"""
    return [to_pg_name(n) for n in get_entity_type_property_names(entity_type)]


def sync_entities_by_fk(odata: Context, db_connection, broken: EntityType, sync_by: EntityType):
    """Not overly generic workaround for buggy servers not able to respond to regular requests.

    As of 2021-12, this workaround is needed for the Voting entities on the Curia Vista server. Unlike all other entity
    types, requesting those does not result in either, a (partial) response, nor in a timeout after 30s. Instead, the
    connection hangs for a few minutes before being reset (on TCP level) by the server.

    Slightly changing the retrieval strategy to specifying $skip/$top works only up to first ~110k entries (of around
    17M). Beyond that, the connection times out as when requesting all entities at once.
    Luckily, for unknown reasons, getting the relevant data is possible when asking for a subset limited by the FK
    relation to the Vote entities. As to expect, this is quite slow however.
    """
    log.info(f'Syncing entity type "{broken.name}" by its references to "{sync_by.name}"')
    entity_set = getattr(odata.client.entity_sets, broken.name)
    with db_connection.cursor() as cursor:
        cursor.execute(f'SELECT id FROM odata.{to_pg_name(sync_by.name)}')
        index = 1
        all_fk = cursor.fetchall()
        for row in all_fk:
            log.info(f'{broken.name} by {sync_by.name}: {index}/{len(all_fk)} done')
            entity_request = entity_set.get_entities()
            entity_request.filter(entity_request.IdVote == row[0])
            for r in entity_request.execute():
                yield r
            index += 1


def fetch_all_entities_by_entity_type(odata: Context, entity_type: EntityType):
    entity_set = getattr(odata.client.entity_sets, entity_type.name)

    next_url = None
    done = 0
    expected_count = None
    while True:
        entities = entity_set.get_entities()
        entity_list = entities.next_url(next_url).count(inline=True).execute()
        done += len(entity_list)
        log.info(f'{entity_type.name}: Got {done} out of {entity_list.total_count}')
        if expected_count is None:
            expected_count = entity_list.total_count
        elif expected_count != entity_list.total_count:
            # Seen for Business
            log.error(f'{entity_type.name}: Total count has changed from {expected_count} to {entity_list.total_count}')
            expected_count = entity_list.total_count
        for entity in entity_list:
            yield entity
        if entity_list.next_url is None:
            break
        next_url = odata.adjust_next_url(entity_list.next_url)
        log.info(f'{entity_type.name}: Fetching next chunk from {next_url}')
    if done != expected_count:
        # Seen for MemberCouncilHistory
        log.error(f'{entity_type.name}: Mismatch of expected and actual number of elements: {expected_count} vs {done}')


def work(context: Context, args, sync_by_fk: Dict[str, str] = {}):
    """Sync of OData into our own database. On conflict, existing data will be overwritten"""
    with database_connection(args) as db_connection:
        ranks = context.get_topology()
        for rank, entity_types_in_rank in enumerate(ranks):
            log.info(f'Fetching rank #{rank}: {", ".join(e.name for e in entity_types_in_rank)}')
            for entity_type in sorted(entity_types_in_rank, key=lambda a: a.name):
                if entity_type in context.skipped_entity_types:
                    log.warning(f'Skipping entity type "{entity_type.name}"')
                    continue
                log.info(f'Fetching all entities of type "{entity_type.name}"')

                # Pick suitable generator for fetching entities
                if entity_type.name in sync_by_fk:
                    sync_by_entity = context.client.schema.entity_type(sync_by_fk[entity_type.name])
                    entities_generator = sync_entities_by_fk(context, db_connection, entity_type, sync_by_entity)
                else:
                    entities_generator = fetch_all_entities_by_entity_type(context, entity_type)

                sql_row_data = []
                sql_column_names = get_gp_column_names_from_entity_type(entity_type)
                for entity in entities_generator:
                    if entity is None:
                        break
                    row = []
                    for property_name in get_entity_type_property_names(entity_type):
                        value = getattr(entity, property_name)
                        row.append(value)
                    sql_row_data.append(row)
                    # Prevent queries from getting overly big
                    if len(sql_row_data) >= 10000:
                        update_db(db_connection, entity_type.name, sql_column_names, sql_row_data)
                        sql_row_data = []
                update_db(db_connection, entity_type.name, sql_column_names, sql_row_data)
