from datetime import timedelta
from time import sleep
from theine.thenie import Cache


def test_set():
    cache = Cache("tlfu", 100, timer="bucket")
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


def test_set_with_ttl_no_timer():
    cache = Cache("tlfu", 100)
    ttl = timedelta(seconds=1)
    ttl2 = timedelta(seconds=3)
    for i in range(20):
        key = f"key:{i}"
        cache.set(key, key, ttl)
    for i in range(20):
        key = f"key:{i}:t2"
        cache.set(key, key, ttl2)
    sleep(1.2)
    for i in range(20):
        key = f"key:{i}"
        result = cache.get(key)
        assert result is None
    for i in range(20):
        key = f"key:{i}:t2"
        result = cache.get(key)
        assert result == key
    sleep(2)
    for i in range(20):
        key = f"key:{i}:t2"
        result = cache.get(key)
        assert result is None


def test_set_with_ttl_bucket_timer():
    ttl = timedelta(seconds=1)
    ttl2 = timedelta(seconds=5)
    cache = Cache("tlfu", 100, timer="bucket")
    for i in range(20):
        key = f"key:{i}"
        cache.set(key, key, ttl)
    for i in range(10):
        key = f"key:{i}:t2"
        cache.set(key, key, ttl2)
    for i in range(20):
        key = f"key:{i}"
        assert cache.get(key) == key
    for i in range(10):
        key = f"key:{i}:t2"
        assert cache.get(key) == key
    sleep(1.2)
    # trigger expire, ttl1 expired, ttl2 not expired
    cache.set("foo", "bar", ttl)
    cache.set("foo:t2", "bar", ttl2)
    # tt2 0-10 & foo & foo:t2
    assert len(cache) == 12
    assert cache.get("foo") == "bar"
    assert cache.get("foo:t2") == "bar"
    for i in range(10):
        key = f"key:{i}:t2"
        assert cache.get(key) == key

    sleep(1.2)
    cache.set("bar", "foo", ttl)
    cache.set("bar:t2", "foo", ttl)
    # tt2 0-10 & foo:t2 & bar & bar:t2
    assert len(cache) == 13
    assert cache.get("bar", "foo")
    for i in range(10):
        key = f"key:{i}:t2"
        assert cache.get(key) == key

    # expire max 20 items each time
    for i in range(40):
        key = f"key:{i}"
        cache.set(key, key, ttl)
    sleep(1.2)
    cache.set("foo", "bar", ttl)
    # ttl 20-40 & tt2 0-10 & foo:t2 & bar & bar:t2 & foo
    assert len(cache) == 34
    sleep(0.1)
    cache.set("bar", "foo", ttl)
    # ttl 0 & tt2 0-10 & foo:t2 & bar & bar:t2 & foo
    assert len(cache) == 15

    # expire ttl2
    sleep(2)
    cache.set("vv", "vv", ttl)
    cache.set("vv:t2", "vv", ttl2)
    # vv & vv:t2 & foo:t2
    assert len(cache) == 3


def test_delete():
    cache = Cache("tlfu", 100, timer="bucket")
    for i in range(20):
        key = f"key:{i}"
        cache.set(key, key)
    cache.delete("key:1")
    cache.delete("key:3")
    cache.delete("key:5")
    assert len(cache) == 17
