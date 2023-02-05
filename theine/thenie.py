import math
import time
from datetime import timedelta
from collections import OrderedDict
from typing import Any, Hashable, NamedTuple, Optional, Dict, Type
from typing_extensions import Protocol
from cacheme_utils import Lru, TinyLfu
from threading import Lock


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


class CachedValue(NamedTuple):
    data: Any
    expire: float


class Cache:
    def __init__(self, policy: str, size: int, ttl: Optional[timedelta] = None):
        self._cache: OrderedDict[Hashable, CachedValue] = OrderedDict()
        self.policy = POLICIES[policy](size)
        self.ttl = ttl.total_seconds() if ttl is not None else math.inf
        self.wait_expire: float = -1
        self.lock = Lock()

    def __len__(self) -> int:
        return len(self._cache)

    def add(self, key: str, value: Any) -> bool:
        cached = self._cache.get(key)
        if cached is not None and cached.expire <= time.time():
            return False
        self.set(key, value)
        return True

    def get(self, key: str, default: Any = None) -> Any:
        self.policy.access(key)
        cached = self._cache.get(key)
        if cached is None:
            return default
        elif cached.expire < time.time():
            self.delete(key)
            return default
        return cached.data

    def set(self, key: str, value: Any):
        now = time.time()
        expire = now + self.ttl
        exist = key in self._cache
        self._cache[key] = CachedValue(value, expire)
        self._cache.move_to_end(key)
        with self.lock:
            if self.wait_expire == -1:
                self.wait_expire = now + self.ttl + 0.01
            if now > self.wait_expire:
                self.expire()
        if exist:
            return
        evicated = self.policy.set(key)
        if evicated is not None:
            self._cache.pop(evicated, None)

    def delete(self, key) -> bool:
        o = object()
        return self._cache.pop(key, o) is not o

    def expire(self):
        remain = 20  # limit maxium proecess size, avoid long blocking
        expiry = time.time()
        expired = []
        self.wait_expire = -1
        for key, item in self._cache.items():
            if remain > 0 and item.expire <= expiry:
                expired.append(key)
                remain -= 1
            else:  # already collect 10 or reach a not expired one
                self.wait_expire = item.expire + 0.01
                break
        for key in expired:
            self._cache.pop(key, None)
            self.policy.remove(key)
