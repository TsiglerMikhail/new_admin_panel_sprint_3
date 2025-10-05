import backoff
from elasticsearch import ConnectionError as ElasticsearchConnectionError
from elasticsearch.helpers import bulk


class ElasticsearchLoader:
    """
    Загрузка данных в ELK
    """
    def __init__(self, elk_conn, state, table):
        self.elk_conn = elk_conn
        self.state = state
        self.table = table

    def _set_id(self, rows):
        for row in rows:
            action = {
                '_id': row['id'],
                '_source': row,
            }
            yield action

    @backoff.on_exception(backoff.expo, (ElasticsearchConnectionError, ), max_time=120)
    def load(self, data, max_modified):
        bulk(client=self.elk_conn, index='movies', actions=self._set_id(data))
        self.state.set_state(self.table.name, str(max_modified))