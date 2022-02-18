import asyncio
import logging
import time

import aiohttp

from odata2sql.benchmark import print_results, EntityTypeSyncResult, set_log_level
from odata2sql.odata import Settings, Context

log = logging.getLogger(__name__)


async def fetch_all_entities_of_type(settings: Settings, entity_type: str):
    async with aiohttp.ClientSession() as session:
        done = 0
        entity_type_begin_time = time.time()
        next_url = f'{settings.url}/{entity_type}?$inlinecount=allpages'
        if odata_filter_for_entity_type := settings.odata_filter_for_entity_type(entity_type):
            next_url += f'&$filter={odata_filter_for_entity_type.replace(" ", "%20")}'
        while True:
            async with session.get(next_url, headers={'Accept': 'application/json'}) as response:
                json = await response.json()
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
        runtime_total = int(entity_type_current_time - entity_type_begin_time)
        log.info(f'{entity_type}: Sync time was {runtime_total}s')
        return EntityTypeSyncResult(entity_type, done, runtime_total)


async def work_main(context: Context):
    start_time = time.time()
    tasks = []
    entity_types = context.include ^ context.skip
    log.info('Entity types to fetch: ' + ', '.join(x.name for x in entity_types))
    for entity_type in entity_types:
        if entity_type.name == 'Voting':
            raise RuntimeError('Benchmarking of entity type "Voting" is not supported')
        tasks.append(asyncio.create_task(fetch_all_entities_of_type(context.settings, entity_type.name)))
    results = await asyncio.gather(*tasks, return_exceptions=True)
    end_time = time.time()
    log.info(f'Total sync time was {int(end_time - start_time)}s')
    print_results(results)


def work(context: Context, args):
    set_log_level(log)
    asyncio.run(work_main(context))
