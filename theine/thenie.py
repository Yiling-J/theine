import inspect
import itertools
import math
import time
import types
from dataclasses import dataclass
from datetime import timedelta
from functools import _make_key, update_wrapper
from threading import Event, Thread
from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    Hashable,
    Optional,
    Type,
    TypeVar,
    cast,
    overload,
)

from theine_core import LruCore, TlfuCore
from typing_extensions import ParamSpec, Protocol, Self

from theine.models import CachedValue

sentinel = object()

_counter = itertools.count()


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


class Key:
    def __init__(self):
        self.key: Optional[str] = None
        self.event = Event()


class Wrapper(Generic[P, R]):
    def __init__(
        self,
        fn: Callable[P, R],
        timeout: Optional[timedelta],
        cache: "Cache",
        coro: bool,
        typed: bool,
        lock: bool,
    ):
        self._key_func: Optional[Callable[..., str]] = None
        self._hk_map: Dict[Hashable, int] = {}
        self._kh_map: Dict[int, Hashable] = {}
        self._events: Dict[Hashable, EventData] = {}
        self._func: Callable = fn
        self._cache: "Cache" = cache
        self._cache._enable_maintenance = False
        self._coro: bool = coro
        self._timeout: Optional[timedelta] = timeout
        self._typed: bool = typed
        self._auto_key: bool = True
        self._lock = lock
        update_wrapper(self, fn)

    def key(self, fn: Callable[P, str]) -> "Wrapper":
        self._key_func = fn
        self._cache._enable_maintenance = True
        self._auto_key = False
        return self

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R:
        if self._auto_key:
            keyh = _make_key(args, kwargs, self._typed)
            v = self._hk_map.get(keyh, -1)
            if v != -1:
                key = f"{v}"
            else:
                if self._lock:
                    event = EventData(Event(), None)
                    ve = self._events.setdefault(keyh, event)
                    if ve is event:
                        uid = next(_counter)
                        key = f"{uid}"
                        self._hk_map[keyh] = uid
                        self._kh_map[uid] = keyh
                        event.data = uid
                        event.event.set()
                        self._events.pop(keyh, None)
                    else:
                        ve.event.wait()
                        key = cast(str, ve.data)
                else:
                    uid = next(_counter)
                    key = f"{uid}"
                    self._hk_map[keyh] = uid
                    self._kh_map[uid] = keyh
        else:
            key = self._key_func(*args, **kwargs)  # type: ignore

        if self._coro:
            result = self._cache.get(key, sentinel)
            if result is sentinel:
                result = CachedAwaitable(self._func(*args, **kwargs))
                evicted = self._cache.set(key, result, self._timeout)
                if self._auto_key and evicted:
                    keyh = self._kh_map.pop(int(evicted), None)
                    if keyh:
                        self._hk_map.pop(keyh, None)
            return cast(R, result)

        data = self._cache.get(key, sentinel)
        if data is not sentinel:
            return cast(R, data)
        if self._lock:
            event = EventData(Event(), None)
            ve = self._events.setdefault(key, event)
            if ve is event:
                result = self._func(*args, **kwargs)
                event.data = result
                self._events.pop(key, None)
                evicted = self._cache.set(key, result, self._timeout)
                if self._auto_key and evicted:
                    keyh = self._kh_map.pop(int(evicted), None)
                    if keyh:
                        self._hk_map.pop(keyh, None)
                event.event.set()
            else:
                ve.event.wait()
                result = ve.data
        else:
            result = self._func(*args, **kwargs)
            evicted = self._cache.set(key, result, self._timeout)
            if self._auto_key and evicted:
                keyh = self._kh_map.pop(int(evicted), None)
                if keyh:
                    self._hk_map.pop(keyh, None)
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
    """
    Memoize decorator to cache function results. This decorator has 2 modes, first one custom-key mode, this
    is also the recommended mode. You must specify the key function manually. Second one is auto-key mode,
    Theine will generate a key for you based on your function inputs.

    :param cache: cache instance from Cache class.
    :param timeout: timedelta to store the function result. Default is None which means no expiration.
    :param typed: Only valid with auto-key mode. If typed is set to true,
        function arguments of different types will be cached separately.
        If typed is false, the implementation will usually regard them as equivalent calls and only cache
        a single result. (Some types such as str and int may be cached separately even when typed is false.)
    :param lock: Cocurrent requests to same data will only fetch from source once. Default is False and
        only make sense if you are using multitheads and want to avoid thundering herd problem.
    """

    def __init__(
        self,
        cache: "Cache",
        timeout: Optional[timedelta],
        typed: bool = False,
        lock: bool = False,
    ):
        self.cache = cache
        self.timeout = timeout
        self.typed = typed
        self.lock = lock

    def __call__(self, fn: Callable[P, R]) -> Wrapper[P, R]:
        coro = inspect.iscoroutinefunction(fn)
        return Wrapper(fn, self.timeout, self.cache, coro, self.typed, self.lock)


class Cache:
    """
    Create new Theine cache store and use API with this class.

    :param policy: eviction policy, "tlfu" and "lru" are the only two supported now.
    :param size: cache size.
    """

    def __init__(self, policy: str, size: int):
        self._cache: Dict[Hashable, CachedValue] = {}
        self.core = CORES[policy](size)
        self._enable_maintenance = True
        self._maintainer = Thread(target=self.maintenance, daemon=True)
        self._maintainer.start()

    def __len__(self) -> int:
        return len(self._cache)

    def get(self, key: str, default: Any = None) -> Any:
        """
        Retrieve data with cache key. If given key is not in cache, return default value.

        :param key: key string.
        :param default: returned value if key is not found in cache, default None.
        """
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
        """
        Add new data to cache. If the key already exists, value will be overwritten.

        :param key: key string.
        :param value: cached value.
        :param ttl: timedelta to store the data Default is None which means no expiration.
        """
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
        """
        Remove key from cache. Return True if given key exists in cache and been deleted.

        :param key: key string.
        """
        v = self._cache.pop(key, sentinel)
        if v is not sentinel:
            self.core.remove(key)
            return True
        return False

    def maintenance(self):
        """
        Remove expired keys.
        """
        while True:
            if self._enable_maintenance:
                self.core.advance(time.time_ns(), self._cache)
            time.sleep(0.5)
