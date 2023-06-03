import time
from typing import Dict


class Timer():
    def __init__(self) -> None:
        self._funcs = {}

    def start(self, function: str) -> None:
        self._funcs[function] = time.time()

    def end(self, function: str) -> None:
        try:
            self._funcs[function] = time.time() - self._funcs[function]
        except KeyError:
            raise ValueError("Invalid function provided")

    @property
    def get_times(self) -> Dict[str, float]:
        return self._funcs
