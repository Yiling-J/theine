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
    sleep(1)
    cache.set("foo", "bar")
    assert len(cache) == 100


def test_set_with_ttl():
    cache = Cache("tlfu", 500)
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
    for i in range(30):
        assert f"key:{i}:2" in cache._cache


def test_delete():
    cache = Cache("tlfu", 100)
    for i in range(20):
        key = f"key:{i}"
        cache.set(key, key)
    cache.delete("key:1")
    cache.delete("key:3")
    cache.delete("key:5")
    assert len(cache) == 17
