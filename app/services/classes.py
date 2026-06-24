from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass
class RedisValue:
    type: str
    value: Any
    expires_at: datetime | None = None

