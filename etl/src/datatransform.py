from src.mapper import ElkFilm


class DataTransform:
    """
    Класс для подготовки данных к загрузке в Elasticsearch
    """
    _columns_for_prepare = ['actors_names', 'directors_names', 'writers_names']

    def transform(self, columns_name, pg_data):
        self.prepare(columns_name, pg_data)
        return [ElkFilm(**row).dict(by_alias=True) for row in pg_data], pg_data[-1][-1]

    def prepare(self, columns_name, pg_data):
        for row in range(0, len(pg_data)):
            for i, value in enumerate(pg_data[row]):
                if columns_name[i] in self._columns_for_prepare:
                    pg_data[row][i] = [c['name'] for c in pg_data[row][i]]
