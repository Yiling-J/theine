from datetime import timedelta
from functools import lru_cache
from random import randint
from typing import Callable, Iterable
from unittest.mock import Mock

from cachetools import LFUCache, cached
from .zipf import Zipf

from theine import Cache, Memoize


def zipf_key_gen() -> Iterable:
    z = Zipf(1.001, 10, 1000000)
    for _ in range(1000000):
        yield f"{z.get()}"


def ucb_key_gen() -> Iterable:
    with open(f"benchmarks/trace/ucb", "rb") as f:
        for line in f:
            vb = line.split(b" ")[-2]
            try:
                v = vb.decode()
            except:
                v = "failed"
            yield v


def ds1_key_gen() -> Iterable:
    with open(f"benchmarks/trace/ds1", "r") as f:
        for line in f:
            yield line.split(",")[0]


def s3_key_gen() -> Iterable:
    with open(f"benchmarks/trace/s3", "r") as f:
        for line in f:
            yield line.split(",")[0]


def bench_theine(policy: str, cap: int, gen: Callable[..., Iterable]):
    @Memoize(Cache(policy, cap), timeout=None)
    def theine_hit(i: str, m: Mock):
        m(i)
        return i

    @theine_hit.key
    def _(i: str, m: Mock) -> str:
        return i

    mock = Mock()
    counter = 0
    for key in gen():
        counter += 1
        v = theine_hit(key, mock)
        assert key == v
    hr = 1 - mock.call_count / counter
    print(f"theine({policy}) hit ratio: {hr:.2f}")
    return hr


def bench_lru(cap: int, gen: Callable[..., Iterable]):
    @lru_cache(maxsize=cap)
    def lru_hit(i: str, m: Mock):
        m(i)
        return i

    mock = Mock()
    counter = 0
    for key in gen():
        v = lru_hit(key, mock)
        assert key == v
        counter += 1
    hr = 1 - mock.call_count / counter
    print(f"lru hit ratio: {hr:.2f}")
    return hr


def bench_cachetools_lfu(cap: int, gen: Callable[..., Iterable]):
    @cached(cache=LFUCache(maxsize=cap))
    def lru_hit(i: str, m: Mock):
        m(i)
        return i

    mock = Mock()
    counter = 0
    for key in gen():
        v = lru_hit(key, mock)
        assert key == v
        counter += 1
    hr = 1 - mock.call_count / counter
    print(f"cachetools lfu hit ratio: {hr:.2f}")
    return hr


def infinit_run(cap: int):
    z = Zipf(1.001, 10, 100000000)
    client = Cache("tlfu", cap)
    count = 0
    hit = 0
    while True:
        key = f"key:{z.get()}"
        data = client.get(key, None)
        if data is None:
            client.set(key, key, timedelta(seconds=randint(30, 20000)))
        else:
            hit += 1
            assert data == key
        count += 1
        if count % 100000 == 0:
            print(f"finish {count // 100000}, hit ratio: {hit / count}")


# infinit_run(50000)

for cap in [100, 200, 500, 1000, 2000, 5000, 10000, 20000]:
    print(f"==== zipf cache size: {cap} ====")
    bench_theine("tlfu", cap, zipf_key_gen)
    bench_cachetools_lfu(cap, zipf_key_gen)
    bench_lru(cap, zipf_key_gen)


for cap in [50000, 100000, 200000, 300000, 500000, 800000, 1000000]:
    print(f"==== ucb cache size: {cap} ====")
    bench_theine("tlfu", cap, ucb_key_gen)
    # bench_cachetools_lfu(cap, ucb_key_gen)
    bench_lru(cap, ucb_key_gen)

for cap in [50000, 100000, 200000, 300000, 500000, 800000, 1000000]:
    print(f"==== ds1 cache size: {cap} ====")
    bench_theine("tlfu", cap, ds1_key_gen)
    # bench_cachetools_lfu(cap, ds1_key_gen)
    bench_lru(cap, ds1_key_gen)

for cap in [50000, 100000, 200000, 300000, 500000, 800000, 1000000]:
    print(f"==== s3 cache size: {cap} ====")
    bench_theine("tlfu", cap, s3_key_gen)
    # bench_cachetools_lfu(cap, s3_key_gen)
    bench_lru(cap, s3_key_gen)
