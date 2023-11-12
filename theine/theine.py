import asyncio
import inspect
import itertools
import time
from dataclasses import dataclass
from datetime import timedelta
from functools import _make_key, update_wrapper
from threading import Event, Thread
from typing import (
    Any,
    Callable,
    Dict,
    Hashable,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
)

from theine_core import ClockProCore, LruCore, TlfuCore
from typing_extensions import ParamSpec, Protocol

from theine.exceptions import InvalidTTL
from theine.models import CacheStats

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

    gen.remove = _remove  # type: ignore
    gen.len = _len  # type: ignore
    gen.kh = kh_map  # type: ignore
    gen.hk = hk_map  # type: ignore
    return gen


class Core(Protocol):
    def __init__(self, size: int):
        ...

    def set(self, key: str, ttl: int) -> Tuple[int, Optional[int], Optional[str]]:
        ...

    def remove(self, key: str) -> Optional[int]:
        ...

    def access(self, key: str) -> Optional[int]:
        ...

    def advance(self, cache: List, sentinel: Any, kh: Dict, hk: Dict):
        ...

    def clear(self):
        ...

    def len(self) -> int:
        ...


class ClockProCoreP(Protocol):
    def __init__(self, size: int):
        ...

    def set(
        self, key: str, ttl: int
    ) -> Tuple[int, Optional[int], Optional[int], Optional[str]]:
        ...

    def remove(self, key: str) -> Optional[int]:
        ...

    def access(self, key: str) -> Optional[int]:
        ...

    def advance(self, cache: List, sentinel: Any, kh: Dict, hk: Dict):
        ...

    def clear(self):
        ...

    def len(self) -> int:
        ...


CORES: Dict[str, Union[Type[Core], Type[ClockProCoreP]]] = {
    "tlfu": TlfuCore,
    "lru": LruCore,
    "clockpro": ClockProCore,
}

P = ParamSpec("P")
R = TypeVar("R", covariant=True)


@dataclass
class EventData:
    event: Event
    data: Any


# https://github.com/python/cpython/issues/90780
# use event to protect from thundering herd
class CachedAwaitable:
    def __init__(self, awaitable):
        self.awaitable = awaitable
        self.future = None
        self.result = sentinel

    def __await__(self):
        if self.result is not sentinel:
            return self.result

        if self.future is None:
            self.future = asyncio.Future()
            result = yield from self.awaitable.__await__()
            self.result = result
            self.future.set_result(self.result)
            self.future = None
            return result
        else:
            yield from self.future.__await__()
        return self.result


class Key:
    def __init__(self):
        self.key: Optional[str] = None
        self.event = Event()


class Cached(Protocol[P, R]):
    _cache: "Cache"

    def key(self, fn: Callable[P, Hashable]):
        ...

    def __call__(self, *args, **kwargs) -> R:
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

    :param policy: eviction policy, "tlfu", "lru" and "clockpro" are the only supported now.
    :param size: cache size.
    """

    def __init__(self, policy: str, size: int):
        self._cache: List[Any] = [sentinel] * (size + 500)
        self.core = CORES[policy](size)
        if policy == "clockpro":
            # clockpro use 2x metadata space, so need to initial 2x space for cache list
            # half of cache list will be sentinel(test page in clock pro)
            self._cache = [sentinel] * (2 * size + 500)
            setattr(self, "set", self._set_clockpro)
        self.key_gen = KeyGen()
        self._closed = False
        self._maintainer = Thread(target=self.maintenance, daemon=True)
        self._maintainer.start()
        self._total = 0
        self._hit = 0
        self.max_size = size

    def __len__(self) -> int:
        return self.core.len()

    def get(self, key: Hashable, default: Any = None) -> Any:
        """
        Retrieve data with cache key. If given key is not in cache, return default value.

        :param key: key hashable, use str/int for best performance.
        :param default: returned value if key is not found in cache, default None.
        """
        self._total += 1
        auto_key = False
        key_str = ""
        if isinstance(key, str):
            key_str = key
        elif isinstance(key, int):
            key_str = f"{key}"
        else:
            key_str = self.key_gen(key)
            auto_key = True

        index = self.core.access(key_str)
        if index is None:
            if auto_key:
                self.key_gen.remove(key_str)
            return default

        self._hit += 1
        return self._cache[index]

    def _access(self, key: Hashable, ttl: Optional[timedelta] = None):
        key_str = ""
        if isinstance(key, str):
            key_str = key
        elif isinstance(key, int):
            key_str = f"{key}"
        else:
            key_str = self.key_gen(key)

        ttl_ns = None
        if ttl is not None:
            seconds = ttl.total_seconds()
            if seconds == 0:
                raise Exception("ttl must be positive")
            ttl_ns = int(seconds * 1e9)
        self.core.set(key_str, ttl_ns or 0)

    def set(
        self, key: Hashable, value: Any, ttl: Optional[timedelta] = None
    ) -> Optional[str]:
        """
        Add new data to cache. If the key already exists, value will be overwritten.

        :param key: key hashable, use str/int for best performance.
        :param value: cached value.
        :param ttl: timedelta to store the data. Default is None which means no expiration. Value smaller than 1 second will round to 1 second. Set a negative value will panic.
        """
        self.core = cast(Core, self.core)
        key_str = ""
        if isinstance(key, str):
            key_str = key
        elif isinstance(key, int):
            key_str = f"{key}"
        else:
            key_str = self.key_gen(key)

        ttl_ns = None
        if ttl is not None:
            seconds = ttl.total_seconds()
            if seconds <= 0:
                raise InvalidTTL("ttl must be positive")
            ttl_ns = int(seconds * 1e9)
        # 0 means no ttl
        index, evicted_index, evicted_key = self.core.set(key_str, ttl_ns or 0)
        self._cache[index] = value
        if evicted_index is not None:
            self._cache[evicted_index] = sentinel
            if evicted_key and evicted_key[:6] == "_auto:":
                self.key_gen.remove(evicted_key)
            return evicted_key
        return None

    # clockpro core set has different set ouput signature
    def _set_clockpro(
        self, key: Hashable, value: Any, ttl: Optional[timedelta] = None
    ) -> Optional[str]:
        """
        Add new data to cache. If the key already exists, value will be overwritten.

        :param key: key hashable, use str/int for best performance.
        :param value: cached value.
        :param ttl: timedelta to store the data. Default is None which means no expiration. Value smaller than 1 second will round to 1 second. Set a negative value will panic.
        """
        self.core = cast(ClockProCore, self.core)
        key_str = ""
        if isinstance(key, str):
            key_str = key
        elif isinstance(key, int):
            key_str = f"{key}"
        else:
            key_str = self.key_gen(key)

        ttl_ns = None
        if ttl is not None:
            # min res 1 second
            seconds = ttl.total_seconds()
            if seconds <= 0:
                raise InvalidTTL("ttl must be positive")
            ttl_ns = int(seconds * 1e9)
        index, test_index, evicted_index, evicted_key = self.core.set(
            key_str, ttl_ns or 0
        )
        self._cache[index] = value
        if test_index is not None:
            self._cache[test_index] = sentinel
        if evicted_index is not None:
            self._cache[evicted_index] = sentinel
            if evicted_key and evicted_key[:6] == "_auto:":
                self.key_gen.remove(evicted_key)
            return evicted_key
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

        index = self.core.remove(key_str)
        if index is not None:
            self._cache[index] = sentinel
            return True
        return False

    def maintenance(self):
        """
        Remove expired keys.
        """
        while not self._closed:
            self.core.advance(self._cache, sentinel, self.key_gen.kh, self.key_gen.hk)
            time.sleep(0.5)

    def clear(self):
        self.core.clear()
        self._cache = [sentinel] * len(self._cache)

    def close(self):
        self._closed = True
        self._maintainer.join()

    def __del__(self):
        self.clear()
        self.close()

    def stats(self) -> CacheStats:
        return CacheStats(self._total, self._hit)
