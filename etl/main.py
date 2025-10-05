import logging.config
import time
from contextlib import closing

import backoff
import elasticsearch
import elasticsearch.helpers
import psycopg2
from elasticsearch.exceptions import ConnectionError
from psycopg2.errors import ConnectionException, OperationalError
from psycopg2.extras import DictCursor

from src.datatransform import DataTransform
from src.elasticsearchloader import ElasticsearchLoader
from src.mapper import TABLES
from src.postgresextractor import PostgresExtractor
from settings import LOGGING_CONFIG, Settings
from state.state import State
from state.storage import JsonFileStorage

settings = Settings()

logging.config.dictConfig(LOGGING_CONFIG)
log = logging.getLogger(__name__)


@backoff.on_exception(backoff.expo, (ConnectionError, ConnectionException, OperationalError), max_time=120)
def execute(state: State):
    with closing(psycopg2.connect(settings.postgres_dsn.unicode_string(), cursor_factory=DictCursor)) as pg_conn, \
            closing(elasticsearch.Elasticsearch(settings.elk_dsn.unicode_string())) as elk_conn:
        datatransformer = DataTransform()
        for table in TABLES:
            postgresextractor = PostgresExtractor(pg_conn, state, table, settings.chunk_size)
            elksearchloader = ElasticsearchLoader(elk_conn, state, table)

            # получаем данные из Postgres
            for columns_name, data in postgresextractor.get_data():
                if data:
                    log.debug(f'Новые записи: {data}')
                    # Если есть изменения, преобразуем данные из формата Postgres в формат, пригодный для Elasticsearch
                    transformed_data, max_modified = datatransformer.transform(data)
                    # загружаем данные в Elasticsearch
                    elksearchloader.load(transformed_data, max_modified)


if __name__ == '__main__':
    log.info('Запуск приложения')

    state = State(storage=JsonFileStorage(file_path=settings.state_file))

    while True:
        try:
            log.info('Запуск очередной итерации')
            execute(state)
        except Exception as e:
            log.error(f'Сбой при выполнении: {e}')
        finally:
            time.sleep(settings.sleep_timeout)
