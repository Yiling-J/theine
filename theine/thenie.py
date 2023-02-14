from collections import defaultdict
from dataclasses import dataclass
import math
import time
import types
import inspect
from datetime import timedelta
from threading import Event, Lock, Thread
from typing import (
    Any,
    Callable,
    DefaultDict,
    Hashable,
    Optional,
    Dict,
    Type,
    cast,
    Generic,
    TypeVar,
    overload,
)
from typing_extensions import Protocol, ParamSpec, Self, Concatenate
from theine_core import LruCore, TlfuCore
from theine.models import CachedValue
from functools import update_wrapper, _make_key
from uuid import uuid4

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

P = ParamSpec("P")
R = TypeVar("R")
_unset = object()


@dataclass
class EventData:
    event: Event
    data: Any


# https://github.com/python/cpython/issues/90780
class CachedAwaitable:
    def __init__(self, awaitable):
        self.awaitable = awaitable
        self.result = _unset

    def __await__(self):
        if self.result is _unset:
            self.result = yield from self.awaitable.__await__()
        return self.result


class Wrapper(Generic[P, R]):
    def __init__(
        self,
        fn: Callable[P, R],
        timeout: Optional[timedelta],
        cache: "Cache",
        coro: bool,
        typed: bool,
    ):
        self._key_func: Optional[Callable] = None
        self._hk_map: Dict[Hashable, str] = {}
        self._kh_map: Dict[str, Hashable] = {}
        self._events: Dict[str, EventData] = {}
        self._func: Callable = fn
        self._cache: "Cache" = cache
        self._cache._enable_maintenance = False
        self._coro: bool = coro
        self._timeout: Optional[timedelta] = timeout
        self._typed: bool = typed
        update_wrapper(self, fn)

    def key(self, fn: Callable[P, str]) -> "Wrapper":
        self._key_func = fn
        self._cache._enable_maintenance = True
        return self

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R:
        key = ""
        auto_key = False
        evicted = None
        if self._key_func is None:
            key_hash = _make_key(args, kwargs, self._typed)
            key_uuid = self._hk_map.setdefault(key_hash, uuid4().hex)
            key = f"{self._func.__name__}:{key_uuid}"
            self._kh_map[key] = key_hash
            auto_key = True
        else:
            key = self._key_func(*args, **kwargs)
        sentinel = object()
        result = self._cache.get(key, sentinel)
        if result is sentinel:
            if self._coro:
                result = CachedAwaitable(self._func(*args, **kwargs))
                evicted = self._cache.set(key, result, self._timeout)
            else:
                event = EventData(Event(), None)
                exist = self._events.setdefault(key, event)
                if event is exist:
                    result = self._func(*args, **kwargs)
                    evicted = self._cache.set(key, result, self._timeout)
                    event.event.set()
                    self._events.pop(key, None)
                else:
                    event.event.wait()
                    result = self._cache.get(key, event.data)
        # remove from hk_map and kh_map same time
        if auto_key and (evicted is not None):
            key_hash = self._kh_map.pop(evicted, None)
            if key_hash is not None:
                self._hk_map.pop(key_hash, None)
        return cast(R, result)

    @overload
    def __get__(self, instance, owner) -> Callable[..., R]:
        ...

    @overload
    def __get__(self, instance, owner) -> Self:  # type: ignore
        ...

    def __get__(self, instance, owner):
        if instance is None:
            return self
        return cast(Callable[..., R], types.MethodType(self, instance))


class Memoize:
    def __init__(
        self, cache: "Cache", timeout: Optional[timedelta], typed: bool = False
    ):
        self.cache = cache
        self.timeout = timeout
        self.typed = typed

    def __call__(self, fn: Callable[P, R]) -> Wrapper[P, R]:
        coro = inspect.iscoroutinefunction(fn)
        return Wrapper(fn, self.timeout, self.cache, coro, self.typed)


class Cache:
    def __init__(self, policy: str, size: int):
        self._cache: Dict[Hashable, CachedValue] = {}
        self.core = CORES[policy](size)
        self._enable_maintenance = True
        self._maintainer = Thread(target=self.maintenance, daemon=True)
        self._maintainer.start()

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

    def set(
        self, key: str, value: Any, ttl: Optional[timedelta] = None
    ) -> Optional[str]:
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
            return None
        evicted = self.core.set_policy(key)
        if evicted is not None:
            self._cache.pop(evicted, None)
            return evicted
        return None

    def delete(self, key: str) -> bool:
        v = self._cache.pop(key, sentinel)
        if v is not sentinel:
            self.core.remove(key)
            return True
        return False

    def maintenance(self):
        while True:
            if self._enable_maintenance:
                self.core.advance(time.time_ns(), self._cache)
            time.sleep(0.5)
