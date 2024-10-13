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
    Generic,
)

from mypy_extensions import KwArg, VarArg
from theine_core import TlfuCore, spread
from typing_extensions import ParamSpec, Protocol, Concatenate


from theine.exceptions import InvalidTTL
from theine.models import CacheStats, Entry, KT, VT
from theine.striped_buffer import StripedBuffer
from theine.write_buffer import WriteBuffer
from theine.utils import round_up_power_of_2
import itertools


S = TypeVar("S", contravariant=True)
P = ParamSpec("P")
R = TypeVar("R", covariant=True, bound=Any)
if TYPE_CHECKING:
    from functools import _Wrapped

sentinel = object()

_maintainer_loop = asyncio.new_event_loop()


def _maintance() -> None:
    asyncio.set_event_loop(_maintainer_loop)
    _maintainer_loop.run_forever()


_maintainer_thread = Thread(target=_maintance, daemon=True)
_maintainer_thread.start()


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
    _cache: "Cache[Hashable, Any]"

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
):
    _key_func = None
    _events = {}
    _func = fn
    _cache = cache
    _timeout = timeout
    _typed = typed
    _auto_key = True

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

        return _cache._get_or_compute(key, lambda: fn(*args, **kwargs))

    fetch._cache = _cache
    _cache.with_loader(fn, timeout)
    fetch.key = key
    return fetch


class Memoize:
    """
    Memoize decorator to cache function results. This decorator has 2 modes, first one custom-key mode, this
    is also the recommended mode. You must specify the key function manually. Second one is auto-key mode,
    Theine will generate a key for you based on your function inputs.

    :param size: cache size.
    :param timeout: timedelta to store the function result. Default is None which means no expiration.
    :param typed: Only valid with auto-key mode. If typed is set to true,
        function arguments of different types will be cached separately.
        If typed is false, the implementation will usually regard them as equivalent calls and only cache
        a single result. (Some types such as str and int may be cached separately even when typed is false.)
    """

    def __init__(
        self,
        size: int,
        timeout: Optional[timedelta],
        typed: bool = False,
    ):
        self.cache = Cache[Hashable, Any](size)
        self.timeout = timeout
        self.typed = typed

    def __call__(self, fn: Callable[Concatenate[S, P], R]) -> Cached[S, P, R]:
        wrapper = Wrapper(fn, self.timeout, self.cache, self.typed)
        return cast(Cached[S, P, R], update_wrapper(wrapper, fn))


class Shard(Generic[KT, VT]):

    def __init__(self) -> None:
        self._map: Dict[KT, Entry[VT]] = {}
        # key map is used to find evicted entries in _map because policy returns hashed key value
        self._key_map: Dict[int, KT] = {}
        self._mutex: Lock = Lock()
        self._hits = itertools.count()
        self._misses = itertools.count()

    def get(self, key: KT, key_hash: int) -> Tuple[Optional[VT], bool]:
        try:
            entry = self._map[key]
            if entry.expire > 0 and entry.expire <= time.monotonic_ns():
                next(self._misses)
                with self._mutex:
                    self._map.pop(key, sentinel)
                    self._key_map.pop(key_hash, sentinel)
                    return (None, False)
            else:
                next(self._hits)
                return (entry.value, True)
        except KeyError:
            next(self._misses)
            return (None, False)

    def _set_nolock(self, key: KT, key_hash: int, value: VT, ttl: int) -> bool:
        # remove exist first if key hash collision
        # not policy update because same hash means same key in policy
        try:
            removed = self._key_map.pop(key_hash)
        except KeyError:
            pass
        else:
            self._map.pop(removed)

        expire = 0
        if ttl > 0:
            expire = time.monotonic_ns() + ttl
        self._map[key] = Entry(value, expire)
        self._key_map[key_hash] = key
        return True

    def set(self, key: KT, key_hash: int, value: VT, ttl: int) -> bool:
        with self._mutex:
            # remove exist first if key hash collision
            # not policy update because same hash means same key in policy
            try:
                removed = self._key_map.pop(key_hash)
            except KeyError:
                pass
            else:
                self._map.pop(removed)

            expire = 0
            if ttl > 0:
                expire = time.monotonic_ns() + ttl
            self._map[key] = Entry(value, expire)
            self._key_map[key_hash] = key
            return True

    def set_ttl(self, key: KT, ttl: int) -> None:
        with self._mutex:
            try:
                entry = self._map[key]
            except KeyError:
                return
            else:
                expire = 0
                if ttl > 0:
                    expire = time.monotonic_ns() + ttl
                entry.expire = expire

    def remove(self, key_hash: int) -> bool:
        with self._mutex:
            try:
                key = self._key_map.pop(key_hash)
            except KeyError:
                return False
            else:
                try:
                    self._map.pop(key)
                except KeyError:
                    return False
                else:
                    return True

    # check expire before remove, used in timer wheel expire only,
    # set(key, ttl1) -> ttl1_sync_to_policy -> set(key, ttl2) -> ttl1 expired -> key_removed -> ttl2_sync_to_policy
    # the key is already removed when policy aware ttl changed, make ttl2 no effect
    def remove_expired(self, key_hash: int) -> None:
        with self._mutex:
            try:
                key = self._key_map[key_hash]
            except KeyError:
                pass
            else:
                try:
                    entry = self._map[key]
                except KeyError:
                    pass
                else:
                    # expired, removed
                    self._key_map.pop(key_hash, sentinel)
                    self._map.pop(key, sentinel)

    def clear(self) -> None:
        with self._mutex:
            self._map.clear()
            self._key_map.clear()

    def __len__(self) -> int:
        with self._mutex:
            return len(self._map)


class Cache(Generic[KT, VT]):
    """
    Create new Theine cache store and use API with this class. This class is not thread-safe.

    :param policy: eviction policy, "tlfu", "lru" and "clockpro" are the only supported now.
    :param size: cache size.
    """

    _loader: Optional[Callable[[KT], VT]]

    _timeout: Optional[timedelta]

    def __init__(self, size: int):
        shard_count = round_up_power_of_2(os.cpu_count() or 4)

        if shard_count < 16:
            shard_count = 16
        elif shard_count > 128:
            shard_count = 128

        self._shards: Tuple[Shard[KT, VT], ...] = tuple(
            [Shard() for _ in range(shard_count)]
        )
        self.core = TlfuCore(size)
        self._closed = False
        self._total = 0
        self._hit = 0
        self.max_size = size
        self._shard_count = shard_count
        self.max_int64 = (1 << 64) - 1
        self._read_buffer = StripedBuffer(self._drain_read)
        self._write_buffer = WriteBuffer(self._drain_write)
        # core is single thread, all core operation must hold this mutex
        self._core_mutex = Lock()
        self._maintainer = asyncio.run_coroutine_threadsafe(
            self.maintenance(), loop=_maintainer_loop
        )
        self._events: Dict[KT, EventData] = {}

    def __len__(self) -> int:
        total = 0
        for shard in self._shards:
            total += len(shard)
        return total

    def with_loader(self, loader: Callable[[KT], VT], ttl: Optional[timedelta]) -> None:
        self._loader = loader
        self._timeout = ttl

    def get(self, key: KT) -> Tuple[Optional[VT], bool]:
        """
        Retrieve data with cache key. If given key is not in cache, return default value.

        :param key: key hashable, use str/int for best performance.
        :param default: returned value if key is not found in cache, default None.
        """
        kh = spread(hash(key))
        (v, ok) = self._shards[kh & (self._shard_count - 1)].get(key, kh)
        self._read_buffer.add(kh)
        return (v, ok)

    def _ttl_nano(self, ttl: Optional[timedelta]) -> int:
        ttl_ns = 0
        if ttl is not None:
            seconds = ttl.total_seconds()
            if seconds == 0:
                raise Exception("ttl must be positive")
            ttl_ns = int(seconds * 1e9)
        return ttl_ns

    def _get_or_compute(self, key: KT, fn: Callable[[], VT]) -> VT:
        kh = spread(hash(key))
        (v, ok) = self._shards[kh & (self._shard_count - 1)].get(key, kh)
        self._read_buffer.add(kh)
        if ok:
            return cast(VT, v)

        if inspect.iscoroutinefunction(fn):
            result = CachedAwaitable(cast(Awaitable[Any], fn()))
            self.set(key, cast(VT, result), self._timeout)
            return cast(VT, result)

        shard = self._shards[kh & (self._shard_count - 1)]
        event = EventData(Event(), None)
        ve = self._events.setdefault(key, event)
        if ve is event:
            # lock the shard, so concatenate write is not allowed
            with shard._mutex:
                result_sync = fn()
                event.data = result_sync
                self._events.pop(key, None)
                ttl_ns = self._ttl_nano(self._timeout)
                shard._set_nolock(key, kh, result_sync, ttl_ns)
            event.event.set()
        else:
            ve.event.wait()
            result_sync = cast(VT, ve.data)
        return result_sync

    def _get_loading(self, key: KT) -> Tuple[Optional[VT], bool]:
        if self._loader is None:
            raise Exception("loader function is None")

        return (
            self._get_or_compute(
                key, cast(Callable[[], VT], lambda: self._loader(key))
            ),
            True,
        )

    def _drain_read(self, keys: List[int]) -> None:
        with self._core_mutex:
            self.core.access(keys)

    def _drain_write(self, entries: List[Tuple[int, int]]) -> None:
        with self._core_mutex:
            evicted = self.core.set(entries)

        # each shard has its own mutex
        for key in evicted:
            self._shards[key & (self._shard_count - 1)].remove(key)

    # send writer buffer to policy immediately, used in test and clear
    def _force_drain_write(self) -> None:
        with self._core_mutex:
            evicted = self.core.set(self._write_buffer.buffer)
            self._write_buffer.buffer = self._write_buffer.buffer[:0]

        for key in evicted:
            self._shards[key & (self._shard_count - 1)].remove(key)

    # used in Django adapter touch method, sets a new expiration for a key
    def _access(self, key: KT, ttl: Optional[timedelta] = None) -> None:
        kh = spread(hash(key))
        ttl_ns = self._ttl_nano(ttl)
        self._shards[kh & (self._shard_count - 1)].set_ttl(key, ttl_ns)
        self._write_buffer.add(kh, ttl_ns)

    def set(self, key: KT, value: VT, ttl: Optional[timedelta] = None) -> None:
        """
        Add new data to cache. If the key already exists, value will be overwritten.

        :param key: key hashable, use str/int for best performance.
        :param value: cached value.
        :param ttl: timedelta to store the data. Default is None which means no expiration. Value smaller than 1 second will round to 1 second. Set a negative value will panic.
        """
        kh = spread(hash(key))

        ttl_ns = self._ttl_nano(ttl)

        self._shards[kh & (self._shard_count - 1)].set(key, kh, value, ttl_ns)
        self._write_buffer.add(kh, ttl_ns)

    def delete(self, key: KT) -> bool:
        """
        Remove key from cache. Return True if given key exists in cache and been deleted.

        :param key: key hashable, use str/int for best performance.
        """
        kh = spread(hash(key))
        success = self._shards[kh & (self._shard_count - 1)].remove(kh)
        self._write_buffer.add(kh, -1)
        return success

    async def maintenance(self) -> None:
        """
        Remove expired keys.
        """
        while True:

            evicted: List[int] = []
            with self._core_mutex:
                if self._closed:
                    return
                evicted = self.core.advance()
            for key in evicted:
                self._shards[key & (self._shard_count - 1)].remove_expired(key)
            await asyncio.sleep(1)

    def clear(self) -> None:
        self._force_drain_write()
        with self._core_mutex:
            self.core.clear()
        for shard in self._shards:
            shard.clear()

    def close(self) -> None:
        with self._core_mutex:
            self._closed = True

    def __del__(self) -> None:
        self.clear()
        self.close()

    def stats(self) -> CacheStats:
        misses = 0
        hits = 0
        for shard in self._shards:
            with shard._mutex:
                hits += next(shard._hits)
                misses += next(shard._misses)
        return CacheStats(hits + misses, hits)
