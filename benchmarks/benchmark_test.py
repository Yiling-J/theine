from datetime import timedelta
from typing import List
import pytest
import uuid
from benchmarks.zipf import Zipf
from theine.thenie import Cache


REQUESTS = 10000


def write_keys(cache: Cache, keys: List[str]):
    for key in keys:
        cache.set(key, key)


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
    cache = Cache(policy="tlfu", size=REQUESTS // 10, ttl=timedelta(seconds=20))

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
