from datetime import timedelta
from typing import List
import uuid
from benchmarks.zipf import Zipf
from theine.thenie import Cache, Memoize
from cachetools import cached, LFUCache


REQUESTS = 10000


def write_keys(cache: Cache, keys: List[str]):
    for key in keys:
        cache.set(key, key, ttl=timedelta(seconds=10))


def read_keys(cache: Cache, keys: List[str]):
    hit = 0
    for key in keys:
        v = cache.get(key)
        if v is not None:
            hit += 1
            assert v == key
    assert hit / len(keys) > 0.9


def test_write(benchmark):
    z = Zipf(1.0001, 10, REQUESTS // 10)
    cache = Cache(policy="tlfu", size=REQUESTS // 10)

    def setup():
        _uuid = uuid.uuid4().int
        return ([f"key:{z.get()}:{_uuid}" for _ in range(REQUESTS)],), {}

    benchmark.pedantic(
        lambda keys: write_keys(cache, keys),
        setup=setup,
        rounds=10,
    )


def test_read(benchmark):
    z = Zipf(1.0001, 10, REQUESTS // 10)
    cache = Cache(policy="tlfu", size=REQUESTS // 10)
    keys = [f"key:{z.get()}" for _ in range(REQUESTS * 3)]
    assert len(set(keys)) > 900
    write_keys(cache, keys)
    assert len(cache) > 900

    def setup():
        return ([f"key:{z.get()}" for _ in range(REQUESTS)],), {}

    benchmark.pedantic(
        lambda keys: read_keys(cache, keys),
        setup=setup,
        rounds=10,
    )


@Memoize(Cache("tlfu", REQUESTS // 10), timeout=None)
def read(key: str):
    return key


@read.key
def _(key: str) -> str:
    return key


def read_keys_memoize(keys: List[str]):
    for key in keys:
        v = read(key)
        if v is not None:
            assert v == key


def test_read_decorator_custom_key(benchmark):
    z = Zipf(1.0001, 10, REQUESTS // 10)
    keys = [f"key:{z.get()}" for _ in range(REQUESTS * 3)]
    read_keys_memoize(keys)

    def setup():
        return ([f"key:{z.get()}" for _ in range(REQUESTS)],), {}

    benchmark.pedantic(
        lambda keys: read_keys_memoize(keys),
        setup=setup,
        rounds=10,
    )


@Memoize(Cache("tlfu", REQUESTS // 10), timeout=None)
def read_auto_key(key: str):
    return key


def read_keys_memoize_auto_key(keys: List[str]):
    for key in keys:
        v = read_auto_key(key)
        if v is not None:
            assert v == key


def test_read_decorator_auto_key(benchmark):
    z = Zipf(1.0001, 10, REQUESTS // 10)
    keys = [f"key:{z.get()}" for _ in range(REQUESTS * 3)]
    read_keys_memoize_auto_key(keys)

    def setup():
        return ([f"key:{z.get()}" for _ in range(REQUESTS)],), {}

    benchmark.pedantic(
        lambda keys: read_keys_memoize_auto_key(keys),
        setup=setup,
        rounds=10,
    )


def test_write_decorator_custom_key(benchmark):
    z = Zipf(1.0001, 10, REQUESTS // 10)

    def setup():
        _uuid = uuid.uuid4().int
        return ([f"key:{z.get()}:{_uuid}" for _ in range(REQUESTS)],), {}

    benchmark.pedantic(
        lambda keys: read_keys_memoize(keys),
        setup=setup,
        rounds=10,
    )


def test_write_decorator_auto_key(benchmark):
    z = Zipf(1.0001, 10, REQUESTS // 10)

    def setup():
        _uuid = uuid.uuid4().int
        return ([f"key:{z.get()}:{_uuid}" for _ in range(REQUESTS)],), {}

    benchmark.pedantic(
        lambda keys: read_keys_memoize_auto_key(keys),
        setup=setup,
        rounds=10,
    )


@cached(cache=LFUCache(maxsize=REQUESTS // 10))
def read_key_cachetools_lfu(key: str):
    return key


def read_keys_memoize_cachetools_lfu(keys: List[str]):
    for key in keys:
        v = read_key_cachetools_lfu(key)
        if v is not None:
            assert v == key


def test_read_decorator_cachetools_lfu(benchmark):
    z = Zipf(1.0001, 10, REQUESTS // 10)
    keys = [f"key:{z.get()}" for _ in range(REQUESTS * 3)]
    read_keys_memoize_cachetools_lfu(keys)

    def setup():
        return ([f"key:{z.get()}" for _ in range(REQUESTS)],), {}

    benchmark.pedantic(
        lambda keys: read_keys_memoize_cachetools_lfu(keys),
        setup=setup,
        rounds=10,
    )


def test_write_decorator_cachetools_lfu(benchmark):
    z = Zipf(1.0001, 10, REQUESTS // 10)

    def setup():
        _uuid = uuid.uuid4().int
        return ([f"key:{z.get()}:{_uuid}" for _ in range(REQUESTS)],), {}

    benchmark.pedantic(
        lambda keys: read_keys_memoize_cachetools_lfu(keys),
        setup=setup,
        rounds=10,
    )
