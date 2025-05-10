import asyncio
import inspect
import itertools
import os
import sysconfig
import time
from dataclasses import dataclass
from datetime import timedelta
from functools import _make_key, update_wrapper
from threading import Event, Lock, Thread
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    Generic,
    Hashable,
    List,
    Optional,
    Tuple,
    TypeVar,
    cast,
    no_type_check,
    overload,
)

from theine_core import TlfuCore, spread
from typing_extensions import Concatenate, ParamSpec, Protocol

from theine.models import KT, VT, CacheStats, Entry
from theine.striped_buffer import StripedBuffer
from theine.utils import round_up_power_of_2
from theine.write_buffer import WriteBuffer

S = TypeVar("S", contravariant=True)
P = ParamSpec("P")

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
    def __init__(
        self, awaitable: Awaitable[Any], on_error: Callable[[BaseException], None]
    ) -> None:
        self.awaitable = awaitable
        self.future: Optional[Awaitable[Any]] = None
        self.result = sentinel
        self.on_error = on_error
        self.exception: Optional[BaseException] = None

    def __await__(self) -> Any:
        if self.result is not sentinel:
            return self.result
        elif self.exception:
            raise self.exception

        if self.future is None:
            try:
                self.future = asyncio.Future()
                result = yield from self.awaitable.__await__()
                self.result = result
                self.future.set_result(self.result)
                self.future = None
                return result
            except BaseException as e:
                self.exception = e
                if self.future:
                    self.future.set_exception(e)
                self.future = None
                self.on_error(e)
                raise e
        else:
            yield from self.future.__await__()
        return self.result


class Key:
    def __init__(self) -> None:
        self.key: Optional[str] = None
        self.event = Event()


class Cached(Protocol[S, P, VT]):
    _cache: "Cache[Hashable, VT]"

    @overload
    def key(self, fn: Callable[P, Hashable]) -> None: ...

    @overload
    def key(self, fn: Callable[Concatenate[S, P], Hashable]) -> None: ...

    @overload
    def __call__(self, _arg_first: S, *args: P.args, **kwargs: P.kwargs) -> VT: ...

    @overload
    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> VT: ...

    def cache_stats(self) -> CacheStats: ...


@no_type_check
def Wrapper(
    fn: Callable,
    ttl: Optional[timedelta],
    cache: "Cache",
    typed: bool,
    is_async: bool,
):
    _key_func = None
    _events = {}
    _func = fn
    _cache = cache
    _ttl = ttl
    _typed = typed
    _auto_key = True
    _is_async = is_async

    def key(fn) -> None:
        nonlocal _key_func
        nonlocal _auto_key
        _key_func = fn
        _auto_key = False

    def cache_stats() -> CacheStats:
        return _cache.stats()

    def fetch(*args, **kwargs):
        if _auto_key:
            key = _make_key(args, kwargs, _typed)
        else:
            key = _key_func(*args, **kwargs)

        if _is_async:
            result, ok = _cache.get(key)
            if not ok:
                result = CachedAwaitable(
                    _func(*args, **kwargs), lambda _: _cache.delete(key)
                )
                _cache.set(key, result, _ttl)
            return result

        return _cache._get_or_compute(key, lambda: fn(*args, **kwargs))

    fetch._cache = _cache
    _cache.with_loader(fn, ttl)
    fetch.key = key
    fetch.cache_stats = cache_stats
    return fetch


class Memoize:
    """
    Memoize decorator to cache function results. This decorator has 2 modes, first one custom-key mode, this
    is also the recommended mode. You must specify the key function manually. Second one is auto-key mode,
    Theine will generate a key for you based on your function inputs.

    :param size: cache size.
    :param ttl: timedelta to store the function result. Default is None which means no expiration.
    :param typed: Only valid with auto-key mode. If typed is set to true,
        function arguments of different types will be cached separately.
        If typed is false, the implementation will usually regard them as equivalent calls and only cache
        a single result. (Some types such as str and int may be cached separately even when typed is false.)
    :param nolock: disables thread locking for get and set operations. Defaults to False. Enable this only if your code does not use multi-threading.
    """

    def __init__(
        self,
        size: int,
        ttl: Optional[timedelta],
        typed: bool = False,
        nolock: bool = False,
    ):
        self.cache = Cache[Hashable, Any](size, nolock)
        self.ttl = ttl
        self.typed = typed

    def __call__(self, fn: Callable[Concatenate[S, P], VT]) -> Cached[S, P, VT]:
        wrapper = Wrapper(
            fn, self.ttl, self.cache, self.typed, inspect.iscoroutinefunction(fn)
        )
        return cast(Cached[S, P, VT], update_wrapper(wrapper, fn))


class Shard(Generic[KT, VT]):

    def __init__(self, nolock: bool) -> None:
        self._map: Dict[KT, Entry[VT]] = {}
        # key map is used to find evicted entries in _map because policy returns hashed key value
        self._key_map: Dict[int, KT] = {}
        self._mutex = Lock()
        self._hits = itertools.count()
        self._misses = itertools.count()
        self._nolock = nolock

    def get(self, key: KT, key_hash: int) -> Tuple[Optional[VT], bool]:
        try:
            # https://docs.python.org/3/howto/free-threading-python.html
            # It’s recommended to use the threading.Lock or other synchronization primitives instead of relying on the internal locks of built-in types, when possible.
            self._nolock or self._mutex.acquire()
            entry = self._map[key]
            if entry.expire > 0 and entry.expire <= time.monotonic_ns():
                next(self._misses)
                self._map.pop(key, sentinel)
                self._key_map.pop(key_hash, sentinel)
                return (None, False)
            else:
                next(self._hits)
                return (entry.value, True)
        except KeyError:
            next(self._misses)
            return (None, False)
        except Exception as err:
            raise err
        finally:
            if not self._nolock:
                self._mutex.release()

    # used only in the cache decorator. self._hits and self._misses are not updated here, as those values are recorded when entering the decorator
    def _get_nolock(self, key: KT, key_hash: int) -> Tuple[Optional[VT], bool]:
        try:
            entry = self._map[key]
            if entry.expire > 0 and entry.expire <= time.monotonic_ns():
                self._map.pop(key, sentinel)
                self._key_map.pop(key_hash, sentinel)
                return (None, False)
            else:
                return (entry.value, True)
        except KeyError:
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
        try:
            self._nolock or self._mutex.acquire()
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
        except Exception as e:
            raise e
        finally:
            if not self._nolock:
                self._mutex.release()

    def set_ttl(self, key: KT, ttl: int) -> None:
        try:
            self._nolock or self._mutex.acquire()
            try:
                entry = self._map[key]
            except KeyError:
                return
            else:
                expire = 0
                if ttl > 0:
                    expire = time.monotonic_ns() + ttl
                entry.expire = expire
        except Exception as e:
            raise e
        finally:
            if not self._nolock:
                self._mutex.release()

    def remove(self, key_hash: int) -> bool:
        try:
            self._nolock or self._mutex.acquire()
            try:
                key = self._key_map.pop(key_hash)
            except KeyError:
                return False
            else:
                try:
                    del self._map[key]
                except KeyError:
                    return False
                else:
                    return True
        except Exception as e:
            raise e
        finally:
            if not self._nolock:
                self._mutex.release()

    # check expire before remove, used in timer wheel expire only,
    # set(key, ttl1) -> ttl1_sync_to_policy -> set(key, ttl2) -> ttl1 expired -> key_removed -> ttl2_sync_to_policy
    # the key is already removed when policy aware ttl changed, make ttl2 no effect
    def remove_expired(self, key_hash: int) -> None:
        try:
            self._nolock or self._mutex.acquire()
            try:
                key = self._key_map[key_hash]
            except KeyError:
                pass
            else:
                try:
                    self._map[key]
                except KeyError:
                    pass
                else:
                    # expired, removed
                    self._key_map.pop(key_hash, sentinel)
                    self._map.pop(key, sentinel)
        except Exception as e:
            raise e
        finally:
            if not self._nolock:
                self._mutex.release()

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

    _ttl: Optional[timedelta]

    def __init__(self, size: int, nolock: bool = False):
        if bool(sysconfig.get_config_var("Py_GIL_DISABLED")):
            nolock = False
        shard_count = round_up_power_of_2(os.cpu_count() or 4)

        if shard_count < 16:
            shard_count = 16
        elif shard_count > 128:
            shard_count = 128

        self._shards: Tuple[Shard[KT, VT], ...] = tuple(
            [Shard(nolock) for _ in range(shard_count)]
        )
        self.core = TlfuCore(size)
        self._closed = False
        self._total = 0
        self._hit = 0
        self.max_size = size
        self._shard_count = shard_count
        self.max_int64 = (1 << 64) - 1
        # core is single thread, all core operation must hold this mutex
        self._core_mutex = Lock()
        self._read_buffer = StripedBuffer(self._drain_read, nolock)
        self._write_buffer = WriteBuffer(
            self._drain_write, self._remove_keys, self._core_mutex, nolock
        )
        self._maintainer = asyncio.run_coroutine_threadsafe(
            self.maintenance(), loop=_maintainer_loop
        )
        self._events: Dict[KT, EventData] = {}
        self._nolock = nolock

    def __len__(self) -> int:
        total = 0
        for shard in self._shards:
            total += len(shard)
        return total

    def with_loader(self, loader: Callable[[KT], VT], ttl: Optional[timedelta]) -> None:
        self._loader = loader
        self._ttl = ttl

    def get(self, key: KT) -> Tuple[Optional[VT], bool]:
        """
        Retrieve data with cache key. If given key is not in cache, return default value.

        :param key: key hashable, use str/int for best performance.
        :param default: returned value if key is not found in cache, default None.
        """
        kh = spread(hash(key))
        idx = kh & (self._shard_count - 1)
        (v, ok) = self._shards[idx].get(key, kh)
        if ok:
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
        shard = self._shards[kh & (self._shard_count - 1)]
        (v, ok) = shard.get(key, kh)
        if ok:
            self._read_buffer.add(kh)
            return cast(VT, v)

        # nolock means single threading mode,
        # where cache stampe problem not exists
        if self._nolock:
            r = fn()
            ttl_ns = self._ttl_nano(self._ttl)
            shard._set_nolock(key, kh, r, ttl_ns)
            self._write_buffer.add(kh, ttl_ns)
            return r

        event = EventData(Event(), None)
        with shard._mutex:
            # check key exists again, because other threads might set the value already
            (v, ok) = shard._get_nolock(key, kh)
            if ok:
                self._read_buffer.add(kh)
                return cast(VT, v)
            ve = self._events.setdefault(key, event)
        # the shard mutex is released when computing value for the key,
        # so following get on same key can wait for the event.
        # it's safe to do so because this method is only used for decorator,
        # and user can not delete/update cache manually when using decorator
        if ve is event:
            result_sync = fn()
            event.data = result_sync
            ttl_ns = self._ttl_nano(self._ttl)
            with shard._mutex:
                shard._set_nolock(key, kh, result_sync, ttl_ns)
                self._events.pop(key, None)
            event.event.set()
            self._write_buffer.add(kh, ttl_ns)
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
        try:
            self._nolock or self._core_mutex.acquire()
            self.core.access(keys)
        except Exception as e:
            raise e
        finally:
            if not self._nolock:
                self._core_mutex.release()

    def _drain_write(self, entries: List[Tuple[int, int]]) -> List[int]:
        return self.core.set(entries)

    def _remove_keys(self, keys: List[int]) -> None:
        for key in keys:
            self._shards[key & (self._shard_count - 1)].remove(key)

    # send writer buffer to policy immediately, used in test and clear
    def _force_drain_write(self) -> None:
        try:
            self._nolock or self._core_mutex.acquire()
            wb = self._write_buffer.buffer[: self._write_buffer.tail]
            if len(wb) > 0:
                evicted = self.core.set(wb)
                for key in evicted:
                    self._shards[key & (self._shard_count - 1)].remove(key)
                self._write_buffer.tail = 0
        except Exception as e:
            raise e
        finally:
            if not self._nolock:
                self._core_mutex.release()

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
