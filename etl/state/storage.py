import abc
import json
from typing import Any, Dict


class BaseStorage(abc.ABC):
    @abc.abstractmethod
    def save_state(self, state: Dict[str, Any]) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def retrieve_state(self) -> Dict[str, Any]:
        raise NotImplementedError


class JsonFileStorage(BaseStorage):
    """Реализация хранилища, использующего локальный файл.
    Формат хранения: JSON
    """

    def __init__(self, file_path: str) -> None:
        self.file_path = file_path

    def save_state(self, state: Dict[str, Any]) -> None:
        with open(self.file_path, 'w') as f:
            json.dump(state, f)

    def retrieve_state(self) -> Dict[str, Any]:
        try:
            with open(self.file_path, 'r') as f:
                storage = json.load(f)
            return storage
        except FileNotFoundError:
            self.save_state({})
            return {}
