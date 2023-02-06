import math
import time
from datetime import timedelta
from collections import OrderedDict
from typing import Any, Hashable, Optional, Dict, Type, cast
from typing_extensions import Protocol
from cacheme_utils import Lru, TinyLfu
from theine.models import CachedValue

from theine.ttl import BucketTimer, FakeTimer

sentinel = object()


class Policy(Protocol):
    def __init__(self, size: int):
        ...

    def set(self, key: str) -> Optional[str]:
        ...

    def remove(self, key: str):
        ...

    def access(self, key: str):
        ...


POLICIES: Dict[str, Type[Policy]] = {
    "tlfu": TinyLfu,
    "lru": Lru,
}


class Cache:
    def __init__(self, policy: str, size: int, timer: str = ""):
        self._cache: OrderedDict[Hashable, CachedValue] = OrderedDict()
        self.policy = POLICIES[policy](size)
        self.timer = FakeTimer()
        if timer == "bucket":
            self.timer = BucketTimer()

    def __len__(self) -> int:
        return len(self._cache)

    def get(self, key: str, default: Any = None) -> Any:
        self.policy.access(key)
        cached = self._cache.get(key)
        if cached is None:
            return default
        elif cached.expire < time.time():
            self.delete(key)
            return default
        return cached.data

    def set(self, key: str, value: Any, ttl: Optional[timedelta] = None):
        now = time.time()
        ts = ttl.total_seconds() if ttl else math.inf
        expire = now + ts
        exist = key in self._cache
        v = CachedValue(value, expire)
        self._cache[key] = v
        self._cache.move_to_end(key)
        if ts != math.inf:
            self.timer.set(key, v, ts)
            expired = self.timer.expire(ts, now)
            for key in expired:
                self._cache.pop(key, None)
                self.policy.remove(key)
        if exist:
            return
        evicated = self.policy.set(key)
        if evicated is not None:
            self._cache.pop(evicated, None)

    def delete(self, key) -> bool:
        v = self._cache.pop(key, sentinel)
        if v != sentinel:
            self.timer.delete(key, cast(CachedValue, v))
            return True
        return False
