from collections import defaultdict
from dataclasses import dataclass
import math
import time
import types
import inspect
from datetime import timedelta
from threading import Event, Thread
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
    key_func: Optional[Callable] = None
    key_map: DefaultDict[Hashable, str] = defaultdict(lambda: uuid4().hex)

    def __init__(
        self,
        fn: Callable[P, R],
        timeout: Optional[timedelta],
        cache: "Cache",
        coro: bool,
        typed: bool,
    ):
        self.func = fn
        self.cache = cache
        self.events: Dict[str, EventData] = {}
        self.coro = coro
        self.timeout = timeout
        self.typed = typed
        update_wrapper(self, fn)

    def key(self, fn: Callable[P, str]) -> "Wrapper":
        self.key_func = fn
        return self

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R:
        key = ""
        if self.key_func is None:
            key_hash = _make_key(args, kwargs, self.typed)
            key = f"{self.func.__name__}:{self.key_map[key_hash]}"
        else:
            key = self.key_func(*args, **kwargs)
        sentinel = object()
        result = self.cache.get(key, sentinel)
        if result is sentinel:
            if self.coro:
                result = CachedAwaitable(self.func(*args, **kwargs))
                self.cache.set(key, result, self.timeout)
            else:
                event = EventData(Event(), None)
                exist = self.events.setdefault(key, event)
                if event is exist:
                    result = self.func(*args, **kwargs)
                    self.cache.set(key, result, self.timeout)
                    event.event.set()
                    self.events.pop(key, None)
                else:
                    event.event.wait()
                    result = self.cache.get(key, event.data)
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
        evicted = self.core.set_policy(key)
        if evicted is not None:
            self._cache.pop(evicted, None)

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
