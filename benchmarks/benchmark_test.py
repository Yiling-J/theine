import uuid
from datetime import timedelta
from typing import List

import pytest
from cachetools import LFUCache, cached

from benchmarks.zipf import Zipf
from theine.thenie import Cache, Memoize

REQUESTS = 10000


def write_keys(cache: Cache, keys: List[str]):
    for key in keys:
        cache.set(key, key, ttl=None)


def read_keys(cache: Cache, keys: List[str]):
    for key in keys:
        v = cache.get(key)
        assert v == key


def test_write(benchmark):
    def setup():
        cache = Cache(policy="tlfu", size=REQUESTS // 10)
        _uuid = uuid.uuid4().int
        return (
            cache,
            [f"key:{i}:{_uuid}" for i in range(REQUESTS)],
        ), {}

    benchmark.pedantic(
        lambda cache, keys: write_keys(cache, keys),
        setup=setup,
        rounds=10,
    )


def test_read(benchmark):
    def setup():
        cache = Cache(policy="tlfu", size=REQUESTS)
        write_keys(cache, [f"key:{i}" for i in range(REQUESTS)])
        return (cache, [f"key:{i}" for i in range(REQUESTS)]), {}

    benchmark.pedantic(
        lambda cache, keys: read_keys(cache, keys),
        setup=setup,
        rounds=10,
    )


def get(key: str):
    return key


def get_many(getter, keys: List[str]):
    for key in keys:
        v = getter(key)
        assert v == key


@pytest.fixture(params=["theine_tlfu_auto", "theine_tlfu_custom", "cachetools_lfu"])
def cache_func_provider(request):
    if request.param == "theine_tlfu_auto":

        def _(size):
            cache = Cache("tlfu", size)
            func = Memoize(cache, None)(get)
            return func

        return _

    elif request.param == "theine_tlfu_custom":

        def _(size):
            cache = Cache("tlfu", size)
            func = Memoize(cache, None)(get)
            func.key(lambda key: key)
            return func

        return _

    elif request.param == "cachetools_lfu":

        def _(size):
            cache = LFUCache(size)
            func = cached(cache)(get)
            return func

        return _

    return None


def test_decorator_read(benchmark, cache_func_provider):
    def setup():
        func = cache_func_provider(REQUESTS)
        keys = [f"key:{i}" for i in range(REQUESTS)]
        for key in keys:
            func(key)
        return (
            func,
            keys,
        ), {}

    benchmark.pedantic(
        lambda func, keys: get_many(func, keys),
        setup=setup,
        rounds=10,
    )


def test_decorator_write(benchmark, cache_func_provider):
    def setup():
        func = cache_func_provider(REQUESTS // 10)
        keys = [f"key:{i}" for i in range(REQUESTS)]
        return (
            func,
            keys,
        ), {}

    benchmark.pedantic(
        lambda func, keys: get_many(func, keys),
        setup=setup,
        rounds=10,
    )


def test_decorator_zipf(benchmark, cache_func_provider):
    def setup():
        z = Zipf(1.0001, 10, REQUESTS)
        func = cache_func_provider(REQUESTS // 10)
        keys = [f"key:{z.get()}" for _ in range(REQUESTS)]
        return (
            func,
            keys,
        ), {}

    benchmark.pedantic(
        lambda func, keys: get_many(func, keys),
        setup=setup,
        rounds=10,
    )
