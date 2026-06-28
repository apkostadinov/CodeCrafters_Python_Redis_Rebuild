from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass
class RedisValue:
    type: str
    value: Any
    expires_at: datetime | None = None

class RedisResponse:
    def __init__(self, value, encoding):
        self.value = value
        self.encoding = encoding

    @property
    def encoding(self):
        return self._encoding if self._encoding else None

    @encoding.setter
    def encoding(self, encoding):
        self._encoding = encoding
