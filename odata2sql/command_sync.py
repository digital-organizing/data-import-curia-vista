import dataclasses
import logging
import multiprocessing
import queue
import sys
from functools import cached_property
from threading import Thread
from timeit import default_timer as timer
from typing import List, Dict, Generator, Union, Optional

import psycopg2
from alive_progress import alive_bar
from psycopg2 import ProgrammingError
from psycopg2.extras import execute_batch

from odata2sql.logging import LogDbHandler
from odata2sql.odata import Context, Settings, get_property_names_of_entity_type
from odata2sql.sql import database_connection, to_pg_name
from pyodata.v2.model import EntityType, ReferentialConstraint

log = logging.getLogger(__name__)

keep_working = True


def do_odata_fetch(settings: Settings, input_: multiprocessing.Queue, output: multiprocessing.Queue):
    """Worker thread function"""
    # Separate instances for every thread
    try:
        # TODO: Re-use already fetched metadata
        context = Context.from_settings(settings)
        while keep_working:
            try:
                work_item = input_.get(timeout=1)
            except queue.Empty:
                continue
            if type(work_item) is Shutdown:
                log.info(f'Shutting down')
                break
            if type(work_item) not in (WorkItemFetchByPrincipal, WorkItemFetchByEntityType):
                log.error(f'Can not process work item {work_item}')
                break
            list_of_entities_generator = work_item.run(context)
            if work_item.selected_property_names:
                property_names = work_item.selected_property_names
                sql_column_names = [to_pg_name(n) for n in work_item.selected_property_names]
            else:
                entity_type = context.get_entity_type_by_name(work_item.entity_type_name)
                property_names = get_property_names_of_entity_type(entity_type)
                sql_column_names = get_gp_column_names_from_entity_type(entity_type)
            for list_of_entities in list_of_entities_generator:
                sql_row_data = []
                for entity in list_of_entities:
                    row = []
                    for property_name in property_names:
                        value = getattr(entity, property_name)
                        row.append(value)
                    sql_row_data.append(row)
                output.put(WorkItemDbPersisting(work_item.entity_type_name, sql_row_data, sql_column_names))
            output.put(work_item)
    except Exception as e:
        output.put(e)
    finally:
        log.info(f'Shutting thread down')


class Shutdown:
    """Sentinel to shut down worker threads"""
    pass


class WorkItemFetchByEntityType:
    """Fetch all items of an OData entity type"""

    def __init__(self, entity_type: EntityType, context: Context):
        self._entity_type_name = entity_type.name
        self._odata_filter = context.odata_filter_for_entity_type(entity_type)
        self._selected_properties_names = context.odata_selected_properties(entity_type)
        self.done_count = 0

    def __str__(self):
        return f'Sync by entity name: {self._entity_type_name}'

    def __eq__(self, other: 'WorkItemFetchByEntityType'):
        return self._entity_type_name == other._entity_type_name

    @cached_property
    def selected_property_names(self):
        return self._selected_properties_names

    @property
    def entity_type_name(self):
        return self._entity_type_name

    def run(self, context) -> Generator:
        entity_set = getattr(context.client.entity_sets, self._entity_type_name)

        next_url = None
        self.done_count = 0
        expected_count = None
        while keep_working:
            request = entity_set.get_entities()
            if self._odata_filter:
                request.filter(self._odata_filter)
            if self._selected_properties_names:
                request.select(','.join(self._selected_properties_names))
            entity_list = request.next_url(next_url).count(inline=True).execute()
            self.done_count += len(entity_list)
            log.debug(f'{self._entity_type_name}: Got {self.done_count} out of {entity_list.total_count} items')
            if expected_count is None:
                expected_count = entity_list.total_count
            elif expected_count != entity_list.total_count:
                # Seen for Curia Vistas 'Business' entities
                log.error(
                    f'{self._entity_type_name}: Total count has changed from {expected_count} to {entity_list.total_count}')
                expected_count = entity_list.total_count
            yield entity_list
            if entity_list.next_url is None:
                break
            next_url = context.adjust_next_url(entity_list.next_url)
            log.debug(f'{self._entity_type_name}: Fetching next chunk from {next_url}')
        if self.done_count != expected_count:
            # Seen for MemberCouncilHistory
            log.error(
                f'{self._entity_type_name}: Mismatch of expected and actual number of elements: {expected_count} vs {self.done_count}')


class WorkItemFetchByPrincipal:
    """Fetch items of an OData entity type by a foreign key. Mainly a workaround for buggy servers.

    As of 2022-02-25, this workaround is needed for the Voting entities on the Curia Vista server. Unlike all other
    entity types, requesting those does not result in either, a (partial) response, nor in a timeout after 30s. Instead,
    the connection hangs for a few minutes before being reset (on TCP level) by the server.

    Slightly changing the retrieval strategy to specifying $skip/$top works only up to first ~110k entries (of around
    17M). Beyond that, the connection times out as when requesting all entities at once.
    Luckily, getting the relevant data is possible when asking for a subset limited by the FK relation to the Vote
    entities. Issuing one request per foreign key tends to be very slow, but in contrast to the regular retrieving
    strategy, it can be parallelized.
    """

    def __init__(self, ref: ReferentialConstraint, foreign_key_property_values: List[str], context: Context):
        if (len(ref.dependent.property_names) != len(foreign_key_property_values)) or len(
                ref.dependent.property_names) == 0:
            raise ValueError(
                '{}: Foreign key properties ({}) mismatch their supposed values ({})'.format(ref.dependent.name,
                                                                                             ', '.join(
                                                                                                 ref.dependent.property_names),
                                                                                             ', '.join(
                                                                                                 foreign_key_property_values)))
        self._entity_type_name = ref.dependent.name
        foreign_key_property_values_as_quoted_string = [str(v) if type(v) == int else f"'{v}'" for v in
                                                        foreign_key_property_values]
        self._odata_filter = " and ".join(
            [f"{k} eq {v}" for k, v in zip(ref.dependent.property_names, foreign_key_property_values_as_quoted_string)])
        if existing_filter := context.odata_filter_for_entity_type(ref.dependent):
            self._odata_filter = f'({existing_filter}) and ({self._odata_filter})'

        try:
            self._selected_properties_names = context.odata_selected_properties(ref.dependent)
        except KeyError:
            self._selected_properties_names = None
        self._principal_name = ref.principal.name
        self.done_count = 0

    @cached_property
    def selected_property_names(self):
        return self._selected_properties_names

    def __str__(self):
        return f'Sync by FK on "{self._principal_name}": "{self._entity_type_name}"'

    def __eq__(self, other: 'WorkItemFetchByEntityType'):
        return self._entity_type_name == other.entity_type_name

    @property
    def entity_type_name(self):
        return self._entity_type_name

    def run(self, context) -> Generator:
        entity_set = getattr(context.client.entity_sets, self._entity_type_name)
        request = entity_set.get_entities().filter(self._odata_filter)
        # TODO: handle partial responses
        entity_list = request.execute()
        self.done_count += len(entity_list)
        yield entity_list


class WorkItemDbPersisting:
    """Describe rows to be put in a database"""

    def __init__(self, entity_type_name: str, rows: List[List[str]], columns: List[str]):
        self._entity_type_name = entity_type_name
        self._columns = columns
        self._rows = rows

    @property
    def entity_type_name(self):
        return self._entity_type_name

    @property
    def total(self):
        return len(self._rows)

    @property
    def rows(self):
        return self._rows

    @property
    def columns(self):
        return self._columns

    @property
    def table_name(self):
        return to_pg_name(self._entity_type_name)

    @property
    def pk_name(self):
        return to_pg_name(self._entity_type_name + '_pkey')


class BacklogInProgressItem:
    """Keep track of work getting done (i.e. work items enqueued, completed entities)"""

    def __init__(self, context: Context, entity_type: EntityType,
                 work_items: List[Union[WorkItemFetchByEntityType, WorkItemFetchByPrincipal]]):
        self.total_count = context.get_entity_type_total_count(entity_type)
        self.done_count = 0
        self.work_items = work_items
        self.entity_type = entity_type
        self.time_begin = timer()

    @property
    def done(self) -> bool:
        """Have all entities been retrieved?

        Please note: Can not check done_count for equality with work_items because certain servers (i.e. Curia Vista)
        actually return a different number of items than what they indicate when using $inlinecount.
        """
        return self.done_count > 0 and len(self.work_items) == 0

    def remove_completed_work_item(self, work_item: Union[WorkItemFetchByEntityType, WorkItemFetchByPrincipal]):
        self.done_count += work_item.done_count
        self.work_items.remove(work_item)

    def log_progress(self):
        log.info(f'Progress for entity type "{self.entity_type}": {self.done_count} out of {self.total_count}')


@dataclasses.dataclass(frozen=True)
class WorkSchedulerProgressState:
    wait_for_dependencies: int
    in_progress: int
    done: int

    def __str__(self):
        return f'Entity types progress: {self.wait_for_dependencies} waiting, {self.in_progress} in progress, {self.done} done'


class WorkScheduler:
    def __init__(self, context: Context, db_connection):
        self._context = context
        self._db_connection = db_connection
        self._odata_work_queue = multiprocessing.Queue()  # Single writer, multiple consumer
        self._odata_result_queue = multiprocessing.Queue()  # Multiple writer, single consumer
        self._backlog_in_progress: Dict[str, BacklogInProgressItem] = {}
        self._backlog_wait_for_dependencies: Dict[str, Optional[str]] = {}
        for et in context.include:
            if principal := context.odata_sync_by_fk(et):
                self._backlog_wait_for_dependencies[et.name] = principal.name
            else:
                self._backlog_wait_for_dependencies[et.name] = None
        self._backlog_done: Dict[str, BacklogInProgressItem] = {}
        self._progress = None

    def _create_progress_state(self):
        pass

    def _enqueue_ready_entity_types(self):
        for entity_type_name in list(self._backlog_wait_for_dependencies.keys()):
            # Skip if dependencies are still outstanding
            if self._backlog_wait_for_dependencies[entity_type_name]:
                continue
            self._enqueue_entity_type(entity_type_name)

    def _enqueue_entity_type(self, entity_type_name: str):
        entity_type = self._context.get_entity_type_by_name(entity_type_name)
        if entity_type_name in self._backlog_wait_for_dependencies:
            del self._backlog_wait_for_dependencies[entity_type_name]
        if principal := self._context.odata_sync_by_fk(entity_type):
            rc = self._context.get_referential_constrain(entity_type, principal)
            with self._db_connection.cursor() as cursor:
                principal_fk_column_names = " ,".join([to_pg_name(n) for n in rc.principal.property_names])
                cursor.execute(f'SELECT {principal_fk_column_names} FROM odata.{to_pg_name(principal.name)}')
                all_fk = cursor.fetchall()
                # TODO: Request more than just one FK per work item
                work_items = [WorkItemFetchByPrincipal(rc, fk, self._context) for fk in all_fk]
        else:
            work_items = [WorkItemFetchByEntityType(entity_type, self._context)]
        backlog_item = self._backlog_in_progress[entity_type_name] = BacklogInProgressItem(self._context, entity_type,
                                                                                           work_items)
        log.info(
            f'Enqueue work for fetching {backlog_item.total_count} entities of type "{entity_type_name}" using {len(work_items)} work items')
        for work_item in work_items:
            self._odata_work_queue.put(work_item)

    def _work_item_done(self, work_item_done: 'WorkItemFetchByEntityType'):
        """Remove @work_item, which is finished by now, from all backlog items. Backlog items without any work items
        left get remove from the backlog."""
        for backlog_item_name in list(self._backlog_in_progress.keys()):
            backlog_item = self._backlog_in_progress[backlog_item_name]
            if work_item_done not in backlog_item.work_items:
                continue
            backlog_item.remove_completed_work_item(work_item_done)
            if not backlog_item.done:
                continue
            del self._backlog_in_progress[backlog_item_name]
            self._backlog_done[backlog_item_name] = backlog_item
            log.info(
                f'Completed entity type "{backlog_item_name}" with {backlog_item.done_count} items after {timer() - backlog_item.time_begin} seconds')
            # Try to enqueue work items which are no longer blocked
            for waiting_entity_type_name in list(self._backlog_wait_for_dependencies.keys()):
                if backlog_item_name == self._backlog_wait_for_dependencies[waiting_entity_type_name]:
                    self._enqueue_entity_type(waiting_entity_type_name)
        self._log_progress_conditionally()

    def _backlog_size(self):
        return len(self._backlog_wait_for_dependencies) + len(self._backlog_in_progress)

    def _log_progress_conditionally(self):
        """If change, log progress"""
        current_progress = WorkSchedulerProgressState(len(self._backlog_wait_for_dependencies),
                                                      len(self._backlog_in_progress),
                                                      len(self._backlog_done))
        if current_progress != self._progress:
            log.info(current_progress)
            self._progress = current_progress
            log.info(
                f'Entity types in progress: {", ".join(self._backlog_in_progress.keys()) if len(self._backlog_in_progress) else "None"}')
            log.info(
                f'Entity types in progress enqueued: {", ".join(self._backlog_wait_for_dependencies.keys()) if len(self._backlog_wait_for_dependencies) else "None"}')

    def total_entities_count(self):
        total_entities = sum(bi.total_count for bi in self._backlog_in_progress.values())
        for entity_type_names in self._backlog_wait_for_dependencies.keys():
            entity_type = self._context.get_entity_type_by_name(entity_type_names)
            total_entities += self._context.get_entity_type_total_count(entity_type)
        return total_entities

    def run(self):
        for i in range(self._context.settings.odata_server_max_connections):
            Thread(target=do_odata_fetch,
                   args=(self._context.settings, self._odata_work_queue, self._odata_result_queue),
                   daemon=True, name=f'OData worker thread #{i}').start()

        self._enqueue_ready_entity_types()
        self._log_progress_conditionally()

        try:
            with alive_bar(self.total_entities_count(), title='Processed items', enrich_print=False) as bar:
                while self._backlog_size():
                    work_item = self._odata_result_queue.get()
                    # The following block relies on DbPersisting items being inserted *before* the WorkItemFetch ones!
                    if type(work_item) is WorkItemDbPersisting:
                        update_db(self._context, self._db_connection, work_item)
                        log.debug(
                            f'Writing {work_item.total} entities of type {work_item.entity_type_name} to database')
                        bar(work_item.total)
                        continue
                    if type(work_item) in (WorkItemFetchByPrincipal, WorkItemFetchByEntityType):
                        self._work_item_done(work_item)
                        continue
                    if type(work_item) is Exception:
                        log.error(f'Error from worker thread: {work_item}')
                        log.error('Shutting down')
                        return
                    log.error(f'Can not process work item {work_item}')
                    break
        except Exception as e:
            while not self._odata_work_queue.empty():
                self._odata_work_queue.get_nowait()
            raise e


def update_db(context: Context, db_connection, work_item: WorkItemDbPersisting):
    """Update multiple values at one. On error, start bisecting until the offending entry(s) got found."""
    table_name_pkey = work_item.pk_name
    statement = (f'INSERT INTO odata.{work_item.table_name} ({", ".join(work_item.columns)})'
                 f' VALUES ({", ".join(["%s"] * len(work_item.columns))}) '
                 f' ON CONFLICT ON CONSTRAINT {table_name_pkey} DO UPDATE SET {", ".join([f"{c} = EXCLUDED.{c}" for c in work_item.columns])};')
    log.debug(f'Running "{statement} on {len(work_item.rows)} rows')
    db_connection.commit()
    with db_connection.cursor() as cur:
        def do_many(rows):
            try:
                execute_batch(cur, statement, rows, page_size=1000)
                db_connection.commit()
                return
            except ProgrammingError as e:
                db_connection.rollback()
                log.fatal(e)
                log.fatal(f"Failed statement: {statement}")
                sys.exit(-1)
            except psycopg2.Error as e:
                db_connection.rollback()
                if len(rows) == 1:
                    log.error(
                        f'Error when inserting data {str(rows[0])} using columns {str(work_item.columns)} to "{work_item.table_name}" : {e}')
                    db_connection.commit()
                    return
            # On failure, retry one by one
            for row in rows:
                do_many([row])

        do_many(work_item.rows)


def get_gp_column_names_from_entity_type(entity_type: EntityType) -> List[str]:
    """SQL column names derived from an entity type"""
    return [to_pg_name(n) for n in get_property_names_of_entity_type(entity_type)]


def work(context: Context, args):
    """Sync of OData into our own database. On conflict, existing data will be overwritten"""

    # Print what we are about to do
    log.info(f'Entity types to sync: {", ".join(e.name for e in context.include)}')
    if context.skip:
        log.warning(f'Entity types to skip: {", ".join(e.name for e in context.skip)}')

    with database_connection(args) as db_connection:
        # Configure logging to database
        db_logger = LogDbHandler(context.session_id, db_connection)
        log.addHandler(db_logger)

        # Add all entity types to WorkManager
        scheduler = WorkScheduler(context, db_connection)
        scheduler.run()
