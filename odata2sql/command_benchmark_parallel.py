import logging
import time
from multiprocessing import Pool
from multiprocessing.pool import ThreadPool

import requests

from odata2sql.benchmark import set_log_level, EntityTypeSyncResult, print_results
from odata2sql.odata import Context

log = logging.getLogger(__name__)


def fetch_all_entities_of_type(service_url, odata_filter, entity_type: str):
    next_url = f'{service_url}/{entity_type}?$inlinecount=allpages'
    if odata_filter:
        next_url += f'&$filter={odata_filter.replace(" ", "%20")}'
    done = 0
    entity_type_begin_time = time.time()
    session = requests.Session()
    while True:
        try:
            json = session.get(url=next_url, headers={'Accept': 'application/json'}).json()
            total = int(json['d']['__count'])
            done += len(json['d']['results'])
            entity_type_current_time = time.time()
            items_per_second = done / (entity_type_current_time - entity_type_begin_time)
            seconds_left = (total - done) / items_per_second
            log.debug(f'{entity_type}: {done}/{total} done, {int(items_per_second)}/s, ETA {seconds_left:.1f}s')

            try:
                next_url = json['d']['__next']
            except KeyError:
                break
        except Exception as e:
            entity_type_current_time = time.time()
            log.error(f'{entity_type}: Failure when fetching {next_url}: {e}')
    runtime_total = int(entity_type_current_time - entity_type_begin_time)
    log.info(f'{entity_type}: Sync time was {runtime_total}s')
    return EntityTypeSyncResult(entity_type, done, runtime_total)


def work_main(context: Context, parallelism_strategy: str):
    start_time = time.time()
    entity_types = context.included_entity_types ^ context.skipped_entity_types
    log.info('Entity types to fetch: ' + ', '.join(x.name for x in entity_types))
    if 'Voting' in [et.name for et in entity_types]:
        raise RuntimeError('Benchmarking of entity type "Voting" is not supported')
    if parallelism_strategy == 'multithreading':
        with ThreadPool() as p:
            results = p.starmap(fetch_all_entities_of_type,
                                [(context.url, context.odata_filter, et.name) for et in entity_types])
    elif parallelism_strategy == 'multiprocessing':
        with Pool() as p:
            results = p.starmap(fetch_all_entities_of_type,
                                [(context.url, context.odata_filter, et.name) for et in entity_types])
    else:
        RuntimeError(f'Invalid parallelism strategy: "{parallelism_strategy}"')
    end_time = time.time()
    log.info(f'Total run time was {int(end_time - start_time)}s')
    print_results(results)


def work(context: Context, args, parallelism_strategy: str):
    set_log_level(log)
    work_main(context, parallelism_strategy)
