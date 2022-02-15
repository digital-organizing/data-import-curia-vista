import dataclasses
import logging
import sys
from typing import Iterable

from tabulate import tabulate


@dataclasses.dataclass
class EntityTypeSyncResult:
    name: str
    count: int
    seconds: int


def print_results(results: Iterable[EntityTypeSyncResult], file=sys.stdout):
    print(tabulate([[x.name, x.count, x.seconds, x.count / x.seconds if x.seconds else x.count] for x in
                    sorted(results, key=lambda x: x.name)],
                   headers=['Entity type', 'Entities', 'Time[s]', 'Entities per second']), file=file)


def set_log_level(log: logging.Logger):
    if log.level == logging.NOTSET or log.level > logging.INFO:
        log.warning(f'Raising logging level "{logging.getLevelName(log.level)}" to "INFO"')
        log.setLevel(logging.INFO)
