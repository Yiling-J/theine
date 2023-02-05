from datetime import timedelta
from time import sleep
from theine.thenie import Cache


def test_set():
    cache = Cache("tlfu", 100)
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


def test_set_with_ttl():
    cache = Cache("tlfu", 100, ttl=timedelta(seconds=1))
    for i in range(20):
        key = f"key:{i}"
        cache.set(key, key)
    for i in range(20):
        key = f"key:{i}"
        assert cache.get(key) == key
    sleep(1.2)
    # trigger expire
    cache.set("foo", "bar")
    assert len(cache) == 1
    assert cache.get("foo", "bar")
    sleep(1.2)
    cache.set("bar", "foo")
    assert len(cache) == 1
    assert cache.get("bar", "foo")
    for i in range(40):
        key = f"key:{i}"
        cache.set(key, key)
    sleep(1.2)
    cache.set("foo", "bar")
    assert len(cache) == 22
    sleep(0.1)
    cache.set("bar", "foo")
    assert len(cache) == 3


def test_delete():
    cache = Cache("tlfu", 100, ttl=timedelta(seconds=1))
    for i in range(20):
        key = f"key:{i}"
        cache.set(key, key)
    cache.delete("key:1")
    cache.delete("key:3")
    cache.delete("key:5")
    assert len(cache) == 17
