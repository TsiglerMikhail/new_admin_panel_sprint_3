from src.mapper import Mapper
from state.state import State


class PostgresExtractor:
    """
    Получение данных из Postgres
    """
    def __init__(self, pg_conn, state: State, table: Mapper, chunk_size: int):
        self.pg_conn = pg_conn
        self.state = state
        self.chunk_size = chunk_size
        self.table = table

    def get_data(self):
        modified = self.state.get_state(self.table.name)
        with self.pg_conn.cursor() as cursor:
            cursor.execute(self.table.query.format(modified=modified))
            columns_name = [column[0] for column in cursor.description]
            while data_chunk := cursor.fetchmany(size=self.chunk_size):
                yield columns_name, data_chunk
