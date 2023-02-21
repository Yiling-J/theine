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


def KeyGen():
    counter = itertools.count()
    hk_map: Dict[Hashable, int] = {}
    kh_map: Dict[int, Hashable] = {}

    def gen(input: Hashable) -> str:
        id = hk_map.get(input, None)
        if id is None:
            id = next(counter)
            hk_map[input] = id
            kh_map[id] = input
        return f"_auto:{id}"

    def _remove(key: str):
        h = kh_map.pop(int(key.replace("_auto:", "")), None)
        if h is not None:
            hk_map.pop(h, None)

    def _len() -> int:
        return len(hk_map)

    gen.remove = _remove
    gen.len = _len
    gen.kh = kh_map
    gen.hk = hk_map
    return gen


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

    def advance(self, now: int, cache: Dict, kh: Dict, hk: Dict):
        ...


CORES: Dict[str, Type[Core]] = {
    "tlfu": TlfuCore,
    "lru": LruCore,
}

P = ParamSpec("P")
R = TypeVar("R", covariant=True)
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


class Cached(Protocol[P, R]):
    _cache: "Cache"

    def key(self, fn: Callable[P, Hashable]):
        ...

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R:
        ...


def Wrapper(
    fn: Callable[P, R],
    timeout: Optional[timedelta],
    cache: "Cache",
    coro: bool,
    typed: bool,
    lock: bool,
) -> Cached[P, R]:

    _key_func: Optional[Callable[..., Hashable]] = None
    _events: Dict[Hashable, EventData] = {}
    _func: Callable = fn
    _cache: "Cache" = cache
    _coro: bool = coro
    _timeout: Optional[timedelta] = timeout
    _typed: bool = typed
    _auto_key: bool = True
    _lock = lock

    def key(fn: Callable[P, Hashable]):
        nonlocal _key_func
        nonlocal _auto_key
        _key_func = fn
        _auto_key = False

    def fetch(*args: P.args, **kwargs: P.kwargs) -> R:
        if _auto_key:
            key = _make_key(args, kwargs, _typed)
        else:
            key = _key_func(*args, **kwargs)  # type: ignore

        if _coro:
            result = _cache.get(key, sentinel)
            if result is sentinel:
                result = CachedAwaitable(_func(*args, **kwargs))
                _cache.set(key, result, _timeout)
            return cast(R, result)

        data = _cache.get(key, sentinel)
        if data is not sentinel:
            return cast(R, data)
        if _lock:
            event = EventData(Event(), None)
            ve = _events.setdefault(key, event)
            if ve is event:
                result = _func(*args, **kwargs)
                event.data = result
                _events.pop(key, None)
                _cache.set(key, result, _timeout)
                event.event.set()
            else:
                ve.event.wait()
                result = ve.data
        else:
            result = _func(*args, **kwargs)
            _cache.set(key, result, _timeout)
        return cast(R, result)

    fetch._cache = _cache  # type: ignore
    fetch.key = key  # type: ignore
    return fetch  # type: ignore


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

    def __call__(self, fn: Callable[P, R]) -> Cached[P, R]:
        coro = inspect.iscoroutinefunction(fn)
        wrapper = Wrapper(fn, self.timeout, self.cache, coro, self.typed, self.lock)
        return update_wrapper(wrapper, fn)


class Cache:
    """
    Create new Theine cache store and use API with this class. This class is not thread-safe.

    :param policy: eviction policy, "tlfu" and "lru" are the only two supported now.
    :param size: cache size.
    """

    def __init__(self, policy: str, size: int):
        self._cache: Dict[str, CachedValue] = {}
        self.core = CORES[policy](size)
        self.key_gen = KeyGen()
        self._maintainer = Thread(target=self.maintenance, daemon=True)
        self._maintainer.start()

    def __len__(self) -> int:
        return len(self._cache)

    def get(self, key: Hashable, default: Any = None) -> Any:
        """
        Retrieve data with cache key. If given key is not in cache, return default value.

        :param key: key hashable, use str/int for best performance.
        :param default: returned value if key is not found in cache, default None.
        """
        auto_key = False
        key_str = ""
        if isinstance(key, str):
            key_str = key
            self.core.access(key_str)
        elif isinstance(key, int):
            key_str = f"{key}"
            self.core.access(key_str)
        else:
            key_str = self.key_gen(key)
            auto_key = True
        cached = self._cache.get(key_str, sentinel)
        if cached is sentinel:
            if auto_key:
                self.key_gen.remove(key_str)
            return default
        elif cast(CachedValue, cached).expire < time.time():
            self.delete(key_str)
            if auto_key:
                self.key_gen.remove(key_str)
            return default
        # For auto generated keys, only access policy if key in cache.
        # Because remove and add back same key will generate a new string key.
        elif auto_key:
            self.core.access(key_str)
        return cast(CachedValue, cached).data

    def set(
        self, key: Hashable, value: Any, ttl: Optional[timedelta] = None
    ) -> Optional[str]:
        """
        Add new data to cache. If the key already exists, value will be overwritten.

        :param key: key hashable, use str/int for best performance.
        :param value: cached value.
        :param ttl: timedelta to store the data. Default is None which means no expiration.
        """
        key_str = ""
        if isinstance(key, str):
            key_str = key
        elif isinstance(key, int):
            key_str = f"{key}"
        else:
            key_str = self.key_gen(key)
        now = time.time()
        ts = max(ttl.total_seconds(), 1.0) if ttl is not None else math.inf
        expire = now + ts
        exist = key in self._cache
        v = CachedValue(value, expire)
        self._cache[key_str] = v
        if expire != math.inf:
            self.core.schedule(key_str, int(expire * 1e9))
        else:
            self.core.deschedule(key_str)
        if exist:
            return None
        evicted = self.core.set_policy(key_str)
        if evicted is not None:
            self._cache.pop(evicted, None)
            if evicted[:6] == "_auto:":
                self.key_gen.remove(evicted)
            return evicted
        return None

    def delete(self, key: Hashable) -> bool:
        """
        Remove key from cache. Return True if given key exists in cache and been deleted.

        :param key: key hashable, use str/int for best performance.
        """
        key_str = ""
        if isinstance(key, str):
            key_str = key
        elif isinstance(key, int):
            key_str = f"{key}"
        else:
            key_str = self.key_gen(key)
            self.key_gen.remove(key_str)
        v = self._cache.pop(key_str, sentinel)
        if v is not sentinel:
            self.core.remove(key_str)
            return True
        return False

    def _delete_str(self, key: str):
        if key[:6] == "_auto:":
            self.key_gen.remove(key)
        self._cache.pop(key, None)

    def maintenance(self):
        """
        Remove expired keys.
        """
        while True:
            self.core.advance(
                time.time_ns(), self._cache, self.key_gen.kh, self.key_gen.hk
            )
            time.sleep(0.5)
