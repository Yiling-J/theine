from dataclasses import dataclass
from typing import Any


@dataclass
class CachedValue:
    data: Any
    expire: float
