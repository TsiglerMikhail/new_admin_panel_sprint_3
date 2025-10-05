from dataclasses import dataclass
from typing import List

from pydantic import BaseModel, Field

base_query = '''SELECT fw.id, fw.rating as imdb_rating, 
        COALESCE(ARRAY_AGG(DISTINCT g.name), ARRAY[]::text[]) AS genres, 
        fw.title, fw.description,
        COALESCE (json_agg(DISTINCT jsonb_build_object('id', p.id, 'name', p.full_name))
        FILTER (WHERE pfw.role = 'director'), '[]') as directors,
        COALESCE (json_agg(DISTINCT p.full_name)
        FILTER (WHERE pfw.role = 'actor'),'[]') as actors_names,
        COALESCE (json_agg(DISTINCT p.full_name)
        FILTER (WHERE pfw.role = 'director'),'[]') as directors_names,
        COALESCE (json_agg(DISTINCT p.full_name)
        FILTER (WHERE pfw.role = 'writer'), '[]') as writers_names,
        COALESCE (json_agg(DISTINCT jsonb_build_object('id', p.id, 'name', p.full_name))
        FILTER (WHERE pfw.role = 'actor'), '[]') as actors,
        COALESCE (json_agg(DISTINCT jsonb_build_object('id', p.id, 'name', p.full_name))
        FILTER (WHERE pfw.role = 'writer'), '[]') as writers,
        {table}.modified as modified
        FROM content.film_work fw
        LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
        LEFT JOIN content.person p ON p.id = pfw.person_id
        LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
        LEFT JOIN content.genre g ON g.id = gfw.genre_id
         '''


class Person(BaseModel):
    id: str
    name: str

class ElkFilm(BaseModel):
    id: str
    imdb_rating: float | None = Field(default=None)
    genres: List[str] = Field(default_factory=list)
    title: str
    description: str | None = Field(default=None)
    directors: List[Person] = Field(default_factory=list)
    actors_names: List[str] = Field(default_factory=list)
    directors_names: List[str] = Field(default_factory=list)
    writers_names: List[str] = Field(default_factory=list)
    actors: List[Person] = Field(default_factory=list)
    writers: List[Person] = Field(default_factory=list)

@dataclass
class Mapper:
    name: str
    query: str


TABLES = [
    Mapper('genre', base_query.format(table='g') +
           "WHERE g.modified > '{modified}' GROUP by g.id, fw.id ORDER BY g.modified"),
    Mapper('person', base_query.format(table='p') +
           "WHERE p.modified > '{modified}' GROUP by p.id, fw.id ORDER BY p.modified"),
    Mapper('film_work', base_query.format(table='fw') +
           "WHERE fw.modified > '{modified}' GROUP by fw.id ORDER BY fw.modified"),
]
