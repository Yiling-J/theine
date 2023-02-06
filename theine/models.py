from dataclasses import dataclass
from typing import Any, Optional, Dict


@dataclass
class CachedValue:
    data: Any
    expire: float
    bucket: Optional[Dict] = None
