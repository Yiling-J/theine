import pytest
from datetime import timedelta
from random import randint
from time import sleep

from theine.thenie import Cache, sentinel


@pytest.fixture(params=["lru", "tlfu", "clockpro"])
def policy(request):
    return request.param


def test_set(policy):
    cache = Cache(policy, 100)
    for i in range(20):
        key = f"key:{i}"
        cache.set(key, key)
    for i in range(20):
        key = f"key:{i}"
        assert cache.get(key) == key
    for i in range(20):
        key = f"key:{i}"
        cache.set(key, key)
    for i in range(20):
        key = f"key:{i}"
        assert cache.get(key) == key
    for i in range(100):
        key = f"key:{i}:v2"
        cache.set(key, key)
    assert len(cache) == 100
    sleep(1)
    cache.set("foo", "bar")
    assert len(cache) == 100


def test_set_cache_size(policy):
    cache = Cache(policy, 500)
    for _ in range(100000):
        i = randint(0, 100000)
        cache.set(f"key:{i}", i)
    assert len([i for i in cache._cache if i is not sentinel]) == 500


def test_set_with_ttl(policy):
    cache = Cache(policy, 500)
    for i in range(30):
        key = f"key:{i}"
        cache.set(key, key, timedelta(seconds=i))
        key = f"key:{i}:2"
        cache.set(key, key, timedelta(seconds=i + 100))
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
    data = [i for i in cache._cache if i is not sentinel]
    for i in range(30):
        assert f"key:{i}:2" in data


def test_delete(policy):
    cache = Cache(policy, 100)
    for i in range(20):
        key = f"key:{i}"
        cache.set(key, key)
    cache.delete("key:1")
    cache.delete("key:3")
    cache.delete("key:5")
    assert len(cache) == 17


class Foo:
    def __init__(self, id):
        self.id = id


def test_hashable_key(policy):
    cache = Cache(policy, 100)
    foos = [Foo(i) for i in range(20)]
    for foo in foos:
        cache.set(foo, foo)
    for foo in foos:
        cached = cache.get(foo, None)
        assert cached is foo
    assert cache.key_gen.len() == 20
    cache.delete(foos[3])
    cached = cache.get(foos[3], None)
    assert cached is None
    assert cache.key_gen.len() == 19


def test_set_with_ttl_hashable(policy):
    cache = Cache(policy, 500)
    foos = [Foo(i) for i in range(30)]
    for i in range(30):
        cache.set(foos[i], foos[i], timedelta(seconds=i))
    assert len(cache) == 30
    assert cache.key_gen.len() == 30
    current = 30
    counter = 0
    while True:
        sleep(5)
        counter += 1
        assert len(cache) < current
        current = len(cache)
        if current == 0:
            break
    assert cache.key_gen.len() == 0


def test_ttl_high_workload(policy):
    cache = Cache(policy, 500000)
    for i in range(500000):
        cache.set((i, 2), i, timedelta(seconds=randint(5, 10)))
    current = 500000
    while True:
        sleep(1)
        assert len(cache) <= current
        current = len(cache)
        if current == 0:
            break
    assert len(cache.key_gen.kh) == 0
    assert len(cache.key_gen.hk) == 0
