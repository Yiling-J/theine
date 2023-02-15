from functools import lru_cache
from unittest.mock import Mock

from cachetools import LFUCache, cached
from zipf import Zipf

from theine import Cache, Memoize


@lru_cache(maxsize=5000)
def lru_hit(i: int, m: Mock):
    m(i)
    return i


def bench_theine(policy: str, cap: int):
    @Memoize(Cache(policy, cap), timeout=None)
    def theine_hit(i: int, m: Mock):
        m(i)
        return i

    @theine_hit.key
    def _(i: int, m: Mock) -> str:
        return f"key:{i}"

    mock = Mock()
    z = Zipf(1.001, 10, 1000000)
    for n in range(1000000):
        num = z.get()
        v = theine_hit(num, mock)
        assert num == v
    print(f"theine({policy}) hit ratio: {1 - mock.call_count / 1000000:.2f}")


def bench_lru(cap: int):
    @lru_cache(maxsize=cap)
    def lru_hit(i: int, m: Mock):
        m(i)
        return i

    mock = Mock()
    z = Zipf(1.001, 10, 1000000)
    for _ in range(1000000):
        num = z.get()
        v = lru_hit(num, mock)
        assert num == v
    print(f"lru hit ratio: {1 - mock.call_count / 1000000:.2f}")


def bench_cachetools_lfu(cap: int):
    @cached(cache=LFUCache(maxsize=cap))
    def lru_hit(i: int, m: Mock):
        m(i)
        return i

    mock = Mock()
    z = Zipf(1.001, 10, 1000000)
    for _ in range(1000000):
        num = z.get()
        v = lru_hit(num, mock)
        assert num == v
    print(f"cachetools lfu hit ratio: {1 - mock.call_count / 1000000:.2f}")


for cap in [100, 200, 500, 1000, 2000, 5000, 10000, 20000]:
    print(f"====== Cache Size {cap} ======")
    bench_theine("tlfu", cap)
    bench_lru(cap)
    bench_cachetools_lfu(cap)
