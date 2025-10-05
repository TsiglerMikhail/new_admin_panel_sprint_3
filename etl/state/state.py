from datetime import datetime
from typing import Any

from state.storage import BaseStorage


class State:
    def __init__(self, storage: BaseStorage) -> None:
        self.storage = storage
        self.cur_state = self.storage.retrieve_state()

    def set_state(self, key: str, value: Any) -> None:
        self.cur_state.update({key: value})
        self.storage.save_state(self.cur_state)

    def get_state(self, key: str) -> Any:
        return self.cur_state.get(key, str(datetime.min))
