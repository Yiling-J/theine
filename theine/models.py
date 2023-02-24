from dataclasses import dataclass
from typing import Any, Optional


@dataclass
class CachedValue:
    data: Any
    expire: Optional[float]
