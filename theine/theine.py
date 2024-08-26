import os
import asyncio
import inspect
import time
from dataclasses import dataclass
from datetime import timedelta
from threading import Lock, Event, Thread
from functools import _make_key, update_wrapper
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    Hashable,
    List,
    Tuple,
    Optional,
    TYPE_CHECKING,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
    overload,
    no_type_check,
)

from mypy_extensions import KwArg, VarArg
from theine_core import ClockProCore, LruCore, TlfuCore
from typing_extensions import ParamSpec, Protocol, Concatenate


from theine.exceptions import InvalidTTL
from theine.models import CacheStats, Entry
from theine.striped_buffer import StripedBuffer
from theine.write_buffer import WriteBuffer
from theine.utils import round_up_power_of_2


S = TypeVar("S", contravariant=True)
P = ParamSpec("P")
R = TypeVar("R", covariant=True, bound=Any)
if TYPE_CHECKING:
    from functools import _Wrapped

sentinel = object()


@dataclass
class EventData:
    event: Event
    data: Any


# https://github.com/python/cpython/issues/90780
# use event to protect from thundering herd
class CachedAwaitable:
    def __init__(self, awaitable: Awaitable[Any]) -> None:
        self.awaitable = awaitable
        self.future: Optional[Awaitable[Any]] = None
        self.result = sentinel

    def __await__(self) -> Any:
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
    def __init__(self) -> None:
        self.key: Optional[str] = None
        self.event = Event()


class Cached(Protocol[S, P, R]):
    _cache: "Cache"

    @overload
    def key(self, fn: Callable[P, Hashable]) -> None: ...

    @overload
    def key(self, fn: Callable[Concatenate[S, P], Hashable]) -> None: ...

    @overload
    def __call__(self, _arg_first: S, *args: P.args, **kwargs: P.kwargs) -> R: ...

    @overload
    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R: ...


@no_type_check
def Wrapper(
    fn: Callable,
    timeout: Optional[timedelta],
    cache: "Cache",
    typed: bool,
    lock: bool,
):
    _key_func = None
    _events = {}
    _func = fn
    _cache = cache
    _timeout = timeout
    _typed = typed
    _auto_key = True
    _lock = lock

    def key(fn) -> None:
        nonlocal _key_func
        nonlocal _auto_key
        _key_func = fn
        _auto_key = False

    def fetch(*args, **kwargs):

        if _auto_key:
            key = _make_key(args, kwargs, _typed)
        else:
            key = _key_func(*args, **kwargs)

        if inspect.iscoroutinefunction(fn):
            result = _cache.get(key, sentinel)
            if result is sentinel:
                result = CachedAwaitable(cast(Awaitable[Any], _func(*args, **kwargs)))
                _cache.set(key, result, _timeout)
            return result

        data = _cache.get(key, sentinel)
        if data is not sentinel:
            return data
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
        return result

    fetch._cache = _cache
    fetch.key = key
    return fetch


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

    def __call__(self, fn: Callable[Concatenate[S, P], R]) -> Cached[S, P, R]:
        wrapper = Wrapper(fn, self.timeout, self.cache, self.typed, self.lock)
        return cast(Cached[S, P, R], update_wrapper(wrapper, fn))


class Shard:

    def __init__(self) -> None:
        self._map: Dict[Any, Entry] = {}
        # key map is used to find evicted entries in _map because policy returns hashed key value
        self._key_map: Dict[int, Any] = {}
        self._mutex: Lock = Lock()
        self._hits: int = 0
        self._misses: int = 0

    def get(self, key: Hashable, default: Any = None) -> Any:
        with self._mutex:
            entry = self._map.get(key)
            if entry is None or (
                entry.expire > 0 and entry.expire <= time.monotonic_ns()
            ):
                self._misses += 1
                return default
            self._hits += 1
            return entry.value

    def set(self, key: Hashable, key_hash: int, value: Any, ttl: int) -> bool:
        with self._mutex:
            # remove exist first if key hash collision
            # not policy update because same hash means same key in policy
            removed = self._key_map.pop(key_hash, sentinel)
            if removed != sentinel:
                self._map.pop(removed)

            expire = 0
            if ttl > 0:
                expire = time.monotonic_ns() + ttl
            self._map[key] = Entry(value, expire)
            self._key_map[key_hash] = key
            return True

    def remove(self, key_hash: int):
        with self._mutex:
            key = self._key_map.pop(key_hash, sentinel)
            if key != sentinel:
                self._map.pop(key)

    def __len__(self) -> int:
        with self._mutex:
            return len(self._map)


class Cache:
    """
    Create new Theine cache store and use API with this class. This class is not thread-safe.

    :param policy: eviction policy, "tlfu", "lru" and "clockpro" are the only supported now.
    :param size: cache size.
    """

    def __init__(self, size: int):
        shard_count = round_up_power_of_2(os.cpu_count())

        if shard_count < 16:
            shard_count = 16
        elif shard_count > 128:
            shard_count = 128

        self._shards: List[Shard] = [Shard() for _ in range(shard_count)]
        self.core = TlfuCore(size)
        self._closed = False
        self._maintainer = Thread(target=self.maintenance, daemon=True)
        self._total = 0
        self._hit = 0
        self.max_size = size
        self._shard_count = shard_count
        self.max_int64 = (1 << 64) - 1
        self._read_buffer = StripedBuffer(self._drain_read)
        self._write_buffer = WriteBuffer(self._drain_write)
        # core is single thread, all core operation must hold this mutex
        self._core_mutex = Lock()

        self._maintainer.start()

    def __len__(self) -> int:
        total = 0
        for shard in self._shards:
            total += len(shard)
        return total

    def get(self, key: Hashable, default: Any = None) -> Any:
        """
        Retrieve data with cache key. If given key is not in cache, return default value.

        :param key: key hashable, use str/int for best performance.
        :param default: returned value if key is not found in cache, default None.
        """
        kh = spread(hash(key))

        v = self._shards[kh & (self._shard_count - 1)].get(key, default)
        self._read_buffer.add(kh)
        return v

    def _drain_read(self, keys: List[int]):
        with self._core_mutex:
            self.core.access(keys)

    def _drain_write(self, entries: List[Tuple[int, int]]):
        with self._core_mutex:
            evicted = self.core.set(entries)

        # each shard has its own mutex
        for key in evicted:
            self._shards[key & (self._shard_count - 1)].remove(key)

    # used in test only, send writer buffer to policy
    def _force_drain_write(self):
        with self._core_mutex:
            evicted = self.core.set(self._write_buffer.buffer)
            self._write_buffer.buffer = self._write_buffer.buffer[:0]

        for key in evicted:
            self._shards[key & (self._shard_count - 1)].remove(key)

    # used in Django adapter touch method, sets a new expiration for a key
    def _access(self, key: Hashable, ttl: Optional[timedelta] = None) -> None:
        kh = spread(hash(key))
        ttl_ns = None
        if ttl is not None:
            seconds = ttl.total_seconds()
            if seconds == 0:
                raise Exception("ttl must be positive")
            ttl_ns = int(seconds * 1e9)
            self._shards[kh & (self._shard_count - 1)].update_ttl(key, ttl_ns)
            self._write_buffer.add(kh, ttl_ns)

    def set(
        self, key: Hashable, value: Any, ttl: Optional[timedelta] = None
    ) -> Optional[str]:
        """
        Add new data to cache. If the key already exists, value will be overwritten.

        :param key: key hashable, use str/int for best performance.
        :param value: cached value.
        :param ttl: timedelta to store the data. Default is None which means no expiration. Value smaller than 1 second will round to 1 second. Set a negative value will panic.
        """
        kh = spread(hash(key))

        ttl_ns = 0
        if ttl is not None:
            seconds = ttl.total_seconds()
            if seconds <= 0:
                raise InvalidTTL("ttl must be positive")
            ttl_ns = int(seconds * 1e9)

        self._shards[kh & (self._shard_count - 1)].set(key, kh, value, ttl_ns)
        self._write_buffer.add(kh, ttl_ns)
        return None

    def delete(self, key: Hashable) -> bool:
        """
        Remove key from cache. Return True if given key exists in cache and been deleted.

        :param key: key hashable, use str/int for best performance.
        """
        kh = spread(hash(key))
        self._shards[kh & (self._shard_count - 1)].remove(kh)
        self._write_buffer.add(kh, -1)
        return False

    def maintenance(self) -> None:
        """
        Remove expired keys.
        """
        while not self._closed:

            evicted: List[int] = []
            with self._core_mutex:
                evicted = self.core.advance()
            for key in evicted:
                self._shards[key & (self._shard_count - 1)].remove(key)

            time.sleep(0.5)

    def clear(self) -> None:
        with self._core_mutex:
            self.core.clear()

    def close(self) -> None:
        self._closed = True
        self._maintainer.join()

    def __del__(self) -> None:
        self.clear()
        self.close()

    def stats(self) -> CacheStats:
        misses = 0
        hits = 0
        for shard in self._shards:
            with shard._mutex:
                hits += shard._hits
                misses += shard._misses
        return CacheStats(hits + misses, hits)
