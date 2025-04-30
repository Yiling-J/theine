import sys
import sysconfig
import threading
from datetime import timedelta
from random import randint
from time import sleep

import pytest
from bounded_zipf import Zipf  # type: ignore[import]

from theine.theine import Cache

is_freethreaded = bool(sysconfig.get_config_var("Py_GIL_DISABLED"))


@pytest.fixture(params=[True, False])
def nolock(request):
    return request.param

def test_set(nolock) -> None:
    cache = Cache(100, nolock)
    for i in range(20):
        key = f"key:{i}"
        cache.set(key, key)
    for i in range(20):
        key = f"key:{i}"
        v, ok = cache.get(key)
        assert ok
        assert v == key
    for i in range(20):
        key = f"key:{i}"
        cache.set(key, key)
    for i in range(20):
        key = f"key:{i}"
        v, ok = cache.get(key)
        assert ok
        assert v == key
    for i in range(100):
        key = f"key:{i}:v2"
        cache.set(key, key)

    cache._force_drain_write()
    assert len(cache) == 100
    sleep(1)
    cache.set("foo", "bar")
    cache._force_drain_write()
    assert len(cache) == 100


def test_set_cache_size(nolock) -> None:
    cache = Cache(500, nolock)
    for _ in range(100000):
        i = randint(0, 100000)
        cache.set(f"key:{i}", i)
    cache._force_drain_write()
    assert len(cache) == 500


def test_set_with_ttl(nolock) -> None:
    cache = Cache(500, nolock)
    for i in range(30):
        key = f"key:{i}"
        cache.set(key, key, timedelta(seconds=i + 1))
        key = f"key:{i}:2"
        cache.set(key, key, timedelta(seconds=i + 100))
    cache._force_drain_write()
    assert len(cache) == 60
    current = 60
    counter = 0

    while True:
        sleep(5)
        counter += 1
        assert len(cache) < current
        current = len(cache)
        if current <= 30:
            break
    assert counter < 10

    for i in range(30):
        v, ok = cache.get(f"key:{i}:2")
        assert ok


def test_set_with_ttl_multi_instances(nolock) -> None:
    caches = []
    for i in range(30):
        caches.append(Cache(500, nolock))

    for cache in caches:
        for i in range(30):
            key = f"key:{i}"
            cache.set(key, key, timedelta(seconds=1 + randint(0, 5)))
        cache._force_drain_write()
        assert len(cache) == 30

    sleep(8)
    for cache in caches:
        assert len(cache) == 0


class SameHash:

    def __init__(self, i):
        self.i = i

    def __hash__(self):
        return 3245671

    def __eq__(self, other):
        return other == self.i


def test_collision(nolock) -> None:
    cache = Cache(500, nolock)
    for i in range(30):
        e = SameHash(i)
        cache.set(e, e, timedelta(seconds=i + 5))
    # only the last entry exists because all entries have same hash
    cache._force_drain_write()
    assert len(cache) == 1
    assert (cache.core.len()) == 1
    obj, ok = cache.get(SameHash(29))
    assert ok
    assert obj.i == 29


def test_delete(nolock) -> None:
    cache = Cache(100, nolock)
    for i in range(20):
        key = f"key:{i}"
        cache.set(key, key)
    cache.delete("key:1")
    cache.delete("key:3")
    cache.delete("key:5")
    cache._force_drain_write()
    assert len(cache) == 17


class Foo:
    def __init__(self, id: int):
        self.id = id


def test_hashable_key(nolock) -> None:
    cache = Cache(100, nolock)
    foos = [Foo(i) for i in range(20)]
    for foo in foos:
        cache.set(foo, foo)
    for foo in foos:
        cached, ok = cache.get(foo)
        assert cached is foo
    cache.delete(foos[3])
    cached, ok = cache.get(foos[3])
    assert not ok


def test_set_with_ttl_hashable(nolock) -> None:
    cache = Cache(500, nolock)
    foos = [Foo(i) for i in range(30)]
    for i in range(30):
        cache.set(foos[i], foos[i], timedelta(seconds=i + 1))
    cache._force_drain_write()
    assert len(cache) == 30
    current = 30
    counter = 0
    while True:
        sleep(5)
        counter += 1
        assert len(cache) < current
        current = len(cache)
        if current == 0:
            break


def test_expire_proactively(nolock) -> None:
    cache = Cache(50000, nolock)
    for i in range(50000):
        cache.set((i, 2), i, timedelta(seconds=randint(5, 10)))
    current = 50000
    cache._force_drain_write()
    while True:
        sleep(1)
        assert len(cache) <= current
        current = len(cache)
        if current == 0:
            break
    assert len(cache.core.keys()) == 0


def test_clear_cache(nolock) -> None:
    cache = Cache(500, nolock)
    for i in range(5000):
        cache.set((i, 2), i, timedelta(seconds=randint(5, 10)))
    sleep(1)

    cache.clear()
    assert len(cache) == 0
    assert cache.core.len() == 0


def test_close_cache(nolock) -> None:
    for _ in range(10):
        cache = Cache(500, nolock)
        cache.set("foo", "bar", timedelta(seconds=60))
        cache.close()
        sleep(3)
        assert cache._maintainer.done() is True


def test_cache_stats(nolock) -> None:
    cache = Cache(5000, nolock)
    assert cache.max_size == 5000
    assert len(cache) == 0
    z = Zipf(1.0001, 10, 20000)
    for _ in range(20000):
        i = z.get()
        key = f"key:{i}"
        v, ok = cache.get(key)
        if not ok:
            cache.set(key, key)
    cache._force_drain_write()
    stats = cache.stats()
    assert stats.hit_count > 0
    assert stats.miss_count > 0
    assert stats.request_count == stats.hit_count + stats.miss_count
    assert stats.hit_rate > 0.5
    assert stats.hit_rate < 1
    assert stats.hit_rate == stats.hit_count / stats.request_count


def zipf_key_gen(total):
    z = Zipf(1.01, 9.0, 50000 * 1000)
    for _ in range(total):
        yield z.get()


def test_zipf(nolock) -> None:
    miss = 0
    cache = Cache(50000, nolock)
    for key in zipf_key_gen(2000000):
        v = cache.get(key)
        if v[1]:
            assert key == v[0]
        else:
            miss += 1
            cache.set(key, key)
    stats = cache.stats()
    assert stats.request_count == 2000000
    assert stats.hit_rate > 0.5 and stats.hit_rate < 0.6
    assert 1 - (miss / 2000000) > 0.5 and 1 - (miss / 2000000) < 0.6


def test_zipf_correctness(nolock) -> None:
    for size in [500, 2000, 10000, 50000]:
        cache = Cache(size, nolock)
        for key in zipf_key_gen(1000000):
            v = cache.get(key)
            if v[1]:
                assert key == v[0]
            else:
                cache.set(key, key)
        cache._force_drain_write()
        total = 0
        for shard in cache._shards:
            total += len(shard._map)
        info = cache.core.debug_info()
        assert total == info.len
        assert info.window_len + info.probation_len + info.protected_len == total
        keys = cache.core.keys()
        assert len(keys) == total
        for kh in cache.core.keys():
            idx = kh & (cache._shard_count - 1)
            assert kh in cache._shards[idx]._key_map


def test_zipf_correctness_parallel() -> None:
    def run(client):
        if is_freethreaded:
            assert sys._is_gil_enabled() is False

        keys = list(zipf_key_gen(500000))
        for key in keys:
            v = client.get(key)
            if v[1]:
                assert key == v[0]
            else:
                client.set(key, key)

    for size in [500, 2000, 10000, 50000]:
        cache = Cache(size)
        threads = []
        for start in range(4):
            thread = threading.Thread(target=run, args=[cache])
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        cache._force_drain_write()
        total = 0
        for shard in cache._shards:
            total += len(shard._map)
        info = cache.core.debug_info()
        assert total == info.len
        assert info.window_len + info.probation_len + info.protected_len == total
        keys = cache.core.keys()
        assert len(keys) == total
        for kh in cache.core.keys():
            idx = kh & (cache._shard_count - 1)
            assert kh in cache._shards[idx]._key_map
