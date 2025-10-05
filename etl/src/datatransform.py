from src.mapper import ElkFilm


class DataTransform:
    """
    Класс для подготовки данных к загрузке в Elasticsearch
    """
    def transform(self, pg_data):
        return [ElkFilm(**row).dict(by_alias=True) for row in pg_data], pg_data[-1][-1]

