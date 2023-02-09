import math
import time
from datetime import timedelta
from threading import Thread
from typing import Any, Hashable, Optional, Dict, Type, cast
from typing_extensions import Protocol
from theine_core import LruCore, TlfuCore
from theine.models import CachedValue

sentinel = object()


class Core(Protocol):
    def __init__(self, size: int):
        ...

    def schedule(self, key: str, expire: int):
        ...

    def deschedule(self, key: str):
        ...

    def set_policy(self, key: str) -> Optional[str]:
        ...

    def set(self, key: str, expire: int) -> Optional[str]:
        ...

    def remove(self, key: str):
        ...

    def access(self, key: str):
        ...

    def advance(self, now: int, cache: dict):
        ...


CORES: Dict[str, Type[Core]] = {
    "tlfu": TlfuCore,
    "lru": LruCore,
}


class Cache:
    def __init__(self, policy: str, size: int):
        self._cache: Dict[Hashable, CachedValue] = {}
        self.core = CORES[policy](size)
        self.maintainer = Thread(target=self.maintenance, daemon=True)
        self.maintainer.start()

    def __len__(self) -> int:
        return len(self._cache)

    def get(self, key: str, default: Any = None) -> Any:
        self.core.access(key)
        cached = self._cache.get(key, sentinel)
        if cached is sentinel:
            return default
        elif cast(CachedValue, cached).expire < time.time():
            self.delete(key)
            return default
        return cast(CachedValue, cached).data

    def set(self, key: str, value: Any, ttl: Optional[timedelta] = None):
        now = time.time()
        ts = max(ttl.total_seconds(), 1.0) if ttl is not None else math.inf
        expire = now + ts
        exist = key in self._cache
        v = CachedValue(value, expire)
        self._cache[key] = v
        if expire != math.inf:
            self.core.schedule(key, int(expire * 1e9))
        else:
            self.core.deschedule(key)
        if exist:
            return
        evicated = self.core.set_policy(key)
        if evicated is not None:
            self._cache.pop(evicated, None)

    def delete(self, key: str) -> bool:
        v = self._cache.pop(key, sentinel)
        if v is not sentinel:
            self.core.remove(key)
            return True
        return False

    def maintenance(self):
        while True:
            self.core.advance(time.time_ns(), self._cache)
            time.sleep(0.5)
