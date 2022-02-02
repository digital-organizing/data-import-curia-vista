import logging
import uuid
from logging import LogRecord

import psycopg2


class LogDBHandler(logging.Handler):
    """ Persist logging messages to database """

    def emit(self, record: LogRecord) -> None:
        with self._connection.cursor() as cursor:
            statement = (f'INSERT INTO private.import_log (session_id, level_number, level_name, message)'
                         f' VALUES (%s, %s, %s, %s)')
            cursor.execute(statement, (self._session_id, record.levelno, record.levelname, record.getMessage()))

    def __init__(self, session_id: uuid, connection, *args, **kwargs):
        psycopg2.extras.register_uuid()
        super().__init__(*args, **kwargs)
        self._session_id = session_id
        self._connection = connection
