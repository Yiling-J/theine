import csv
from datetime import timedelta
from functools import lru_cache
from random import randint
from time import sleep
from typing import Callable, Iterable
from unittest.mock import Mock

import matplotlib.pyplot as plt
from bounded_zipf import Zipf
from cachetools import LFUCache, cached

from theine import Cache, Memoize

plt.style.use("ggplot")


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


def scarab_key_gen() -> Iterable:
    with open("benchmarks/trace/sc.trace", "rb") as f:
        while True:
            raw = f.read(8)
            if not raw:
                break
            yield str(int.from_bytes(raw, "big"))


def fb_key_gen() -> Iterable:
    with open("benchmarks/trace/fb.csv", "r", newline="") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            if row["op"] != "GET":
                continue
            yield str(row["key"])


def bench_theine(policy: str, cap: int, gen: Callable[..., Iterable]):
    counter = 0
    load = 0

    @Memoize(Cache(policy, cap), timeout=None)
    def theine_hit(i: str):
        nonlocal load
        load += 1
        return i

    @theine_hit.key
    def _(i: str) -> str:
        return i

    for key in gen():
        counter += 1
        v = theine_hit(key)
        assert key == v
        if counter % 100000 == 0:
            print(".", end="", flush=True)
    print("")
    hr = (counter - load) / counter
    print(f"---- theine({policy}) hit ratio: {hr:.3f}")
    return hr


def bench_lru(cap: int, gen: Callable[..., Iterable]):
    counter = 0
    load = 0

    @lru_cache(maxsize=cap)
    def lru_hit(i: str):
        nonlocal load
        load += 1
        return i

    for key in gen():
        counter += 1
        v = lru_hit(key)
        assert key == v
        if counter % 100000 == 0:
            print(".", end="", flush=True)
    print("")
    hr = (counter - load) / counter
    print(f"---- lru hit ratio: {hr:.3f}")
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


def init_plot(title):
    fig, ax = plt.subplots()
    ax.set_xlabel("capacity")
    ax.set_ylabel("hit ratio")
    ax.set_title(title)
    fig.set_figwidth(12)
    return fig, ax


def bench_and_plot(caps, key_gen, name):
    x = []
    y_tlfu = []
    y_clockpro = []
    y_lru = []

    for cap in caps:
        print(f"======= {name} cache size: {cap} =======")
        x.append(cap)
        y_tlfu.append(bench_theine("tlfu", cap, key_gen))
        y_clockpro.append(bench_theine("clockpro", cap, key_gen))
        y_lru.append(bench_theine("lru", cap, key_gen))

    fig, ax = init_plot(f"Hit Ratios - {name}")
    ax.plot(x, y_tlfu, "bo-", label="w-tlfu")
    ax.plot(x, y_clockpro, "r*-", label="clockpro")
    ax.plot(x, y_lru, "yh-", label="lru")
    ax.legend()
    fig.savefig(f"benchmarks/{name.lower()}.png", dpi=200)


# infinit_run(50000)

bench_and_plot([100, 200, 500, 1000, 2000, 5000, 10000, 20000], zipf_key_gen, "Zipf")
bench_and_plot(
    [50000, 100000, 200000, 300000, 500000, 800000, 1000000], ucb_key_gen, "UCB"
)
bench_and_plot(
    [50000, 100000, 200000, 300000, 500000, 800000, 1000000], ds1_key_gen, "DS1"
)
bench_and_plot(
    [50000, 100000, 200000, 300000, 500000, 800000, 1000000], s3_key_gen, "S3"
)
bench_and_plot(
    [1000, 2000, 5000, 10000, 20000, 50000, 100000], scarab_key_gen, "SCARAB"
)
bench_and_plot(
    [50000, 100000, 200000, 300000, 500000, 800000, 1000000], fb_key_gen, "FB"
)
