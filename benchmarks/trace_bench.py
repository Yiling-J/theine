import concurrent.futures
import gzip
import struct
from datetime import timedelta
from random import randint
from threading import Thread
from typing import Callable, Iterator, List

import matplotlib.pyplot as plt
import numpy as np
import zstandard as zstd
from bounded_zipf import Zipf
from cachetools import FIFOCache, LRUCache
from scipy.interpolate import PchipInterpolator

from theine import Cache

plt.style.use("ggplot")


GET = "GET"
SET = "SET"


def zipf_key_gen(name: str) -> Iterator:
    z = Zipf(1.001, 10, 50000000)
    for _ in range(1000000):
        yield f"{z.get()}", GET


def arc_key_gen(name: str) -> Iterator:
    with gzip.open(f"benchmarks/trace/{name.lower()}.gz", "rt") as f:
        for line in f:
            tmp = line.split(" ")
            base = int(tmp[0])
            count = int(tmp[1])
            for i in range(count):
                yield str(base + i), GET


def oracle_general_gen(name: str) -> Iterator:
    path = ""
    if name == "WIKI CDN":
        path = "benchmarks/trace/wiki_2019t.oracleGeneral.zst"
    if name == "Twitter Cluster 52":
        path = "benchmarks/trace/cluster52.oracleGeneral.sample10.zst"
    with open(path, "rb") as f:
        dctx = zstd.ZstdDecompressor()
        with dctx.stream_reader(f) as reader:
            buffer_size = 24
            buffer = bytearray(buffer_size)

            while True:
                try:
                    num_read = reader.readinto(buffer)
                    if num_read < buffer_size:
                        return
                except EOFError:
                    return
                except Exception as err:
                    raise Exception(f"Wrapped error: {err}")

                # Interpret bytes 4 through the end as a little-endian unsigned 64-bit integer
                id = struct.unpack_from("<Q", buffer, offset=4)[0]
                yield str(id), GET


def lirs_key_gen(name: str) -> Iterator:
    with gzip.open("benchmarks/trace/loop.gz", "rt") as f:
        for line in f:
            k = line.strip()
            yield k, GET


def corda_key_gen(name: str) -> Iterator:
    buffer_size = 8
    buffer = bytearray(buffer_size)
    with gzip.open("benchmarks/trace/trace_vaultservice_large.gz", "rb") as reader:
        while True:
            try:
                num_read = reader.readinto(buffer)
                if num_read < buffer_size:
                    return
            except EOFError:
                return
            except Exception as err:
                raise Exception(f"Wrapped error: {err}")

            id = struct.unpack_from(">Q", buffer)[0]
            yield str(id), GET


def bench_theine(cap: int, gens: List[Callable[..., Iterator]], name: str):
    counter = 0
    miss = 0

    cache = Cache(cap, True)

    for gen in gens:
        for key, op in gen(name):
            counter += 1
            if op == GET:
                v = cache.get(key)
                if v[1]:
                    assert key == v[0]
                else:
                    miss += 1
                    cache.set(key, key)
            else:
                cache.set(key, key)

            if counter % 100000 == 0:
                print(".", end="", flush=True)
    print("")
    hr = (counter - miss) / counter
    print(f"---- theine hit ratio: {hr:.3f}")
    return hr


def bench_cachetools(
    policy: str, cap: int, gens: List[Callable[..., Iterator]], name: str
):
    counter = 0
    miss = 0

    if policy == "LRU":
        cache = LRUCache(cap)
    elif policy == "FIFO":
        cache = FIFOCache(cap)

    for gen in gens:
        for key, op in gen(name):
            counter += 1
            if op == GET:
                v = cache.get(key)
                if v is not None:
                    assert key == v
                else:
                    miss += 1
                    cache[key] = key
            else:
                cache[key] = key

            if counter % 100000 == 0:
                print(".", end="", flush=True)
    print("")
    hr = (counter - miss) / counter
    print(f"---- cachetools {policy} hit ratio: {hr:.3f}")
    return hr


def _infinit_run(cache: Cache[int, int]):
    z = Zipf(1.001, 10, 100000000)
    count = 0
    while True:
        key = z.get()
        data, exists = cache.get(key)
        if not exists:
            cache.set(key, key, timedelta(seconds=randint(30, 20000)))
        else:
            assert data == key
        count += 1
        if count % 100000 == 0:
            print(".", end="", flush=True)


def infinit_run_parallel(cap: int):
    cache = Cache(500000)

    threads = []
    for _ in range(6):
        thread = Thread(target=_infinit_run, args=[cache])
        thread.start()
        threads.append(thread)
    for t in threads:
        t.join()


def init_plot(title):
    fig, ax = plt.subplots()
    ax.set_xlabel("capacity")
    ax.set_ylabel("hit ratio")
    ax.set_title(title)
    fig.set_figwidth(12)
    return fig, ax


def bench_and_plot(caps, key_gens, name):
    x = []
    y_tlfu = []
    y_cachetools_lru = []
    y_cachetools_fifo = []

    for cap in caps:
        print(f"======= {name} cache size: {cap} =======")
        x.append(cap)
        y_tlfu.append(bench_theine(cap, key_gens, name))
        y_cachetools_lru.append(bench_cachetools("LRU", cap, key_gens, name))
        y_cachetools_fifo.append(bench_cachetools("FIFO", cap, key_gens, name))

    fig, ax = init_plot(f"Hit Ratios - {name}")
    # Smooth the lines using np.interp for interpolation
    x_new = np.linspace(min(x), max(x), 300)

    tlfu_spl = PchipInterpolator(x, y_tlfu)
    clru_spl = PchipInterpolator(x, y_cachetools_lru)
    cfifo_spl = PchipInterpolator(x, y_cachetools_fifo)
    y_tlfu_smooth = tlfu_spl(x_new)
    y_clru_smooth = clru_spl(x_new)
    y_cfifo_smooth = cfifo_spl(x_new)

    ax.plot(x_new, y_tlfu_smooth, "b-", label="theine")
    ax.plot(x_new, y_clru_smooth, "r-", label="cachetools-lru")
    ax.plot(x_new, y_cfifo_smooth, "c-", label="cachetools-fifo")
    ax.plot(x, y_tlfu, "bo")
    ax.plot(x, y_cachetools_lru, "ro")
    ax.plot(x, y_cachetools_fifo, "co")

    ax.legend()
    fig.savefig(f"benchmarks/{name.lower()}.png", dpi=200)


def bench_and_plot_parallel(caps, key_gens, name):
    x = []
    y_tlfu = []
    y_cachetools_lru = []
    y_cachetools_fifo = []

    futures = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=12) as executor:
        for cap in caps:
            futures[executor.submit(bench_theine, cap, key_gens, name)] = (
                "theine",
                cap,
            )
            futures[executor.submit(bench_cachetools, "LRU", cap, key_gens, name)] = (
                "lru",
                cap,
            )
            futures[executor.submit(bench_cachetools, "FIFO", cap, key_gens, name)] = (
                "fifo",
                cap,
            )

        m = {}
        for future in concurrent.futures.as_completed(futures):
            tp, cap = futures[future]
            data = future.result()
            if cap not in m:
                m[cap] = [0, 0, 0]
            if tp == "theine":
                m[cap][0] = data
            elif tp == "lru":
                m[cap][1] = data
            elif tp == "fifo":
                m[cap][2] = data

    keys = list(m.keys())
    keys.sort()
    for key in keys:
        x.append(key)
        v = m[key]
        y_tlfu.append(v[0])
        y_cachetools_lru.append(v[1])
        y_cachetools_fifo.append(v[2])

    fig, ax = init_plot(f"Hit Ratios - {name}")
    # Smooth the lines using np.interp for interpolation
    x_new = np.linspace(min(x), max(x), 300)

    tlfu_spl = PchipInterpolator(x, y_tlfu)
    clru_spl = PchipInterpolator(x, y_cachetools_lru)
    cfifo_spl = PchipInterpolator(x, y_cachetools_fifo)
    y_tlfu_smooth = tlfu_spl(x_new)
    y_clru_smooth = clru_spl(x_new)
    y_cfifo_smooth = cfifo_spl(x_new)

    ax.plot(x_new, y_tlfu_smooth, "b-", label="theine")
    ax.plot(x_new, y_clru_smooth, "r-", label="cachetools-lru")
    ax.plot(x_new, y_cfifo_smooth, "c-", label="cachetools-fifo")
    ax.plot(x, y_tlfu, "bo")
    ax.plot(x, y_cachetools_lru, "ro")
    ax.plot(x, y_cachetools_fifo, "co")

    ax.legend()
    fig.savefig(f"benchmarks/{name.lower()}.png", dpi=200)


# infinit_run_parallel(50000)

bench_and_plot(
    [500, 1000, 2000, 5000, 10_000, 20_000, 40_000, 80_000], [zipf_key_gen], "Zipf"
)
bench_and_plot(
    [
        1_000_000,
        2_000_000,
        3_000_000,
        4_000_000,
        5_000_000,
        6_000_000,
        7_000_000,
        8_000_000,
    ],
    [arc_key_gen],
    "DS1",
)
bench_and_plot(
    [100_000, 200_000, 300_000, 400_000, 500_000, 600_000, 700_000, 800_000],
    [arc_key_gen],
    "S3",
)

bench_and_plot(
    [25_000, 50_000, 100_000, 200_000, 300_000, 400_000, 500_000, 600_000],
    [arc_key_gen],
    "P3",
)

bench_and_plot(
    [10_000, 20_000, 30_000, 40_000, 50_000, 60_000, 70_000, 80_000],
    [arc_key_gen],
    "P8",
)

bench_and_plot(
    [250, 500, 750, 1000, 1250, 1500, 1750, 2000],
    [arc_key_gen],
    "OLTP",
)

bench_and_plot(
    [250, 500, 750, 1000, 1250, 1500, 1750, 2000],
    [lirs_key_gen],
    "LIRS",
)

bench_and_plot(
    [256, 512, 1024, 2048],
    [corda_key_gen],
    "VAULT",
)

bench_and_plot(
    [256, 512, 1024, 2048],
    [
        corda_key_gen,
        lirs_key_gen,
        lirs_key_gen,
        lirs_key_gen,
        lirs_key_gen,
        lirs_key_gen,
        corda_key_gen,
    ],
    "MIX",
)

bench_and_plot(
    [500, 1000, 1500, 2000],
    [oracle_general_gen],
    "Twitter Cluster 52",
)
